use std::time::Duration;

use async_channel::{Receiver, Sender, unbounded};
use log::error;
use tokio::{
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
    modules: Vec<JoinHandle<()>>,
    // Channel to inform when the system is shut down.
    finish_tx: Sender<()>,
    finish_rx: Receiver<()>,
}

impl System {
    /// Register a given module in the system. Return a `ModuleRef`, which can
    /// be used then to send messages to the module.
    pub async fn register_module<T: Module>(&mut self, mut module: T) -> ModuleRef<T> {
        let (tx, rx) = unbounded();
        let module_ref = ModuleRef {
            tx,
            finish_rx: self.finish_rx.clone(),
        };
        self.modules.push(tokio::spawn({
            let module_ref = module_ref.clone();
            let finish_rx = self.finish_rx.clone();
            async move { loop {
                // Should be cancellation safe with 2 unbounded recv
                tokio::select! {
                    biased;
                    _ = finish_rx.recv() => {
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
        let modules = Vec::new();
        let (finish_tx, finish_rx) = unbounded();
        Self {
            modules,
            finish_tx,
            finish_rx,
        }
    }

    /// Gracefully shut the system down.
    ///
    /// The shutdown waits for all already started handlers to finish and for
    /// all registered modules to be dropped. It does not wait for all enqueued
    /// messages to be handled. It does not wait for all Tokio tasks to finish,
    /// but it causes all of them to finish (for example, a task handling
    /// `ModuleRef::request_tick()` may finish an interval after the shutdown).
    /// It is undefined what happens when the system is used after shutdown.
    /// However, a shutdown will not cause any panics in handlers or Tokio
    /// tasks.
    ///
    /// Not cancel-safe.
    pub async fn shutdown(&mut self) {
        // Signal the shutdown for everybody.
        self.finish_tx.close();

        // Await module completion.
        let mut panic = false;
        for module in self.modules.drain(..) {
            if let Err(err) = module.await {
                error!("A module closure failed joining, reason: {err}");
                if err.is_panic() {
                    panic = true;
                }
            }
        }
        // Panic if a registered module panics.
        if panic {
            panic!("A module panicked!");
        }
    }
}

/// A reference to a module used for sending messages.
pub struct ModuleRef<T: Module + ?Sized> {
    tx: Sender<Box<dyn Handlee<T>>>,
    finish_rx: Receiver<()>,
}

impl<T: Module> ModuleRef<T> {
    /// Send a given message to the module.
    pub async fn send<M: Message>(&self, msg: M)
    where
        T: Handler<M>,
    {
        if let Err(err) = self.tx.send(Box::new(msg)).await {
            error!("ModuleRef trying to send a message to a closed channel: {err}");
        }
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
        let mut finish = false;
        while !finish {
            // Should be cancellation safe with tokio::tick and async_channel recv
            tokio::select! {
                biased;
                f = rx.recv() => {
                    if f.is_ok() {
                        finish = true;
                    } else {
                        break;
                    }
                }
                _ = self.finish_rx.recv() => {
                    finish = true;
                }
                _ = interval.tick() => {
                    self.send(message.clone()).await;
                }
            };
        }
        // If the TimerHandle was dropped, we continue without waiting for `stop`.
        while !finish {
            // Should be cancellation safe with tokio::tick and async_channel recv
            tokio::select! {
                biased;
                _ = self.finish_rx.recv() => {
                    finish = true;
                }
                _ = interval.tick() => {
                    self.send(message.clone()).await;
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
            finish_rx: self.finish_rx.clone(),
        }
    }
}
