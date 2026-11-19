use std::sync::Arc;
use std::time::Duration;

use async_channel::{Receiver, Sender, unbounded};
use log::error;
use tokio::{
    sync::Mutex,
    task::JoinHandle,
    time::{Instant, interval_at},
};

pub trait Message: Send + 'static {}
impl<T: Send + 'static> Message for T {}

pub trait Module: Send + 'static {}
impl<T: Send + 'static> Module for T {}

/// A trait for modules capable of handling messages of type `M`.
#[async_trait::async_trait]
pub trait Handler<M: Message>: Module {
    /// Handle the message. A module must be able to access a `ModuleRef` to
    /// itself through `self_ref`.
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, msg: M);
}

// A trait for messages that can be handled by a module of type T.
#[async_trait::async_trait]
trait Handlee<T: Module>: Message {
    async fn get_handled(self: Box<Self>, module_ref: &ModuleRef<T>, t: &mut T);
}

#[async_trait::async_trait]
impl<M: Message, T: Handler<M>> Handlee<T> for M {
    async fn get_handled(self: Box<Self>, module_ref: &ModuleRef<T>, t: &mut T) {
        t.handle(module_ref, *self).await
    }
}

/// A handle returned by `ModuleRef::request_tick()`, can be used to stop
/// sending further ticks.
pub struct TimerHandle {
    stop_ticks: Sender<()>,
}

impl TimerHandle {
    /// Stop the sending of ticks resulting from the corresponding call to
    /// `ModuleRef::request_tick()`.
    /// If the ticks are already stopped, do nothing.
    pub async fn stop(&self) {
        // Do nothing if the channel is already closed.
        let _ = self.stop_ticks.send(()).await;
    }
}

pub struct System {
    modules: Arc<Mutex<Vec<JoinHandle<()>>>>,
    finish_tx: Sender<()>,
    finish_rx: Receiver<()>,
    timer_handles: Arc<Mutex<Vec<TimerHandle>>>,
}

impl System {
    /// Register a given module in the system. Return a `ModuleRef`, which can
    /// be used then to send messages to the module.
    pub async fn register_module<T: Module>(&mut self, mut module: T) -> ModuleRef<T> {
        let (tx, rx) = unbounded();
        let timer_handles = Arc::clone(&self.timer_handles);
        let module_ref = ModuleRef { tx, timer_handles };
        let mut modules = self.modules.lock().await;
        modules.push(tokio::spawn({
            let module_ref = module_ref.clone();
            let finish_rx = self.finish_rx.clone();
            async move { loop {
                tokio::select! {
                    biased;

                    f = finish_rx.recv() => {
                        f.expect("`finished` channel shouldn't close");
                        break;
                    }
                    msg = rx.recv() => {
                        let msg = msg
                            .expect("Message channel cannot close unless all ModuleRefs are dropped, and this closure owns one.");
                        msg.get_handled(&module_ref, &mut module).await;
                    }
                };
            }}
        }));
        module_ref
    }

    /// Create and start a new instance of the system.
    pub async fn new() -> Self {
        let modules = Arc::new(Mutex::new(Vec::new()));
        let (finish_tx, finish_rx) = unbounded();
        let timer_handles = Arc::new(Mutex::new(Vec::new()));
        Self {
            modules,
            finish_tx,
            finish_rx,
            timer_handles,
        }
    }

    /// Gracefully shut the system down.
    pub async fn shutdown(&mut self) {
        let cnt = self.finish_tx.receiver_count();
        // Signal the shutdown for everybody.
        // TODO think of data races when someone registers a module
        for _ in 1..cnt {
            // Leave 1 finish_rx alive for further cloning when registering modules.
            self.finish_tx
                .send(())
                .await
                .expect("Module closure cannot finish on its own");
        }

        // Shutdown tickers.
        {
            let mut timer_handles = self.timer_handles.lock().await;
            for handle in timer_handles.drain(..) {
                handle.stop().await;
            }
        }

        // Await module completion.
        {
            let mut modules = self.modules.lock().await;
            for m in modules.drain(..) {
                if let Err(err) = m.await {
                    error!("A module closure failed joining, reason: {err}");
                    if err.is_panic() {
                        panic!("A module panicked!");
                    }
                }
            }
        }
    }
}

/// A reference to a module used for sending messages.
pub struct ModuleRef<T: Module + ?Sized> {
    tx: Sender<Box<dyn Handlee<T>>>,
    timer_handles: Arc<Mutex<Vec<TimerHandle>>>,
}

impl<T: Module> ModuleRef<T> {
    /// Send a given message to the module.
    pub async fn send<M: Message>(&self, msg: M)
    where
        T: Handler<M>,
    {
        self.tx
            .send(Box::new(msg))
            .await
            .expect("Sending a message after the system shut down.");
    }

    /// Schedule a message to be sent to the module periodically with the given
    /// interval. The first tick is sent after the interval elapses.
    /// Every call to this function results in sending new ticks and does not
    /// cancel ticks resulting from previous calls.
    pub async fn request_tick<M>(&self, message: M, delay: Duration) -> TimerHandle
    where
        M: Message + Clone,
        T: Handler<M>,
    {
        let (tx, rx) = unbounded();
        tokio::spawn(self.clone().tick(message, delay, rx));
        let mut timer_handles = self.timer_handles.lock().await;
        timer_handles.push(TimerHandle {
            stop_ticks: tx.clone(),
        });
        TimerHandle { stop_ticks: tx }
    }

    // Task, which handles sending ticks to a module.
    async fn tick<M>(self, message: M, delay: Duration, rx: Receiver<()>)
    where
        M: Message + Clone,
        T: Handler<M>,
    {
        let start = Instant::now() + delay;
        let mut interval = interval_at(start, delay);

        loop {
            tokio::select! {
                biased;

                f = rx.recv() => {
                    f.expect("`finish` channel shouldn't close on System's end");
                    break;
                }
                _ = interval.tick() => {
                    let m = message.clone();
                    self.send(m).await;
                }
            };
        }
    }
}

impl<T: Module> Clone for ModuleRef<T> {
    /// Create a new reference to the same module.
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            timer_handles: Arc::clone(&self.timer_handles),
        }
    }
}
