use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use async_channel::{Receiver, Sender, unbounded};
use log::error;
use tokio::{
    sync::{Mutex, Notify},
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

type Modules = Arc<Mutex<Vec<Box<dyn Mod>>>>;

pub struct System {
    // Modules whose executors were not launched yet.
    modules: Modules,
    finished_or_mod_registered: Arc<Notify>,
    is_finished: Arc<AtomicBool>,
    executor: Option<JoinHandle<()>>,
    finished: Arc<Notify>,
    ticker: Option<JoinHandle<()>>,
    ticking_modules: Arc<Mutex<Vec<Sender<()>>>>,
}

#[async_trait::async_trait]
trait Mod: Module {
    async fn execute(self: Box<Self>, finished: Receiver<()>);
}

impl System {
    /// Register a given module in the system. Return a `ModuleRef`, which can
    /// be used then to send messages to the module.
    pub async fn register_module<T: Module>(&mut self, module: T) -> ModuleRef<T> {
        let (tx, rx) = unbounded();
        let ticking_modules = Arc::clone(&self.ticking_modules);
        let mod_ref = ModuleRef {
            tx,
            ticking_modules,
        };
        {
            let mut modules = self.modules.lock().await;
            modules.push(Box::new(ModuleExecutor {
                module_ref: mod_ref.clone(),
                module,
                rx,
            }));
        }
        self.finished_or_mod_registered.notify_one();
        mod_ref
    }

    /// Create and start a new instance of the system.
    pub async fn new() -> Self {
        let modules = Arc::new(Mutex::new(Vec::new()));
        let finished_or_mod_registered = Arc::new(Notify::new());
        let finished = Arc::new(Notify::new());
        let is_finished = Arc::new(AtomicBool::new(false));
        let ticking_modules = Arc::new(Mutex::new(Vec::new()));

        let executor = tokio::spawn(Self::execute(
            Arc::clone(&modules),
            Arc::clone(&finished_or_mod_registered),
            Arc::clone(&is_finished),
        ));

        let ticker = tokio::spawn(Self::shutdown_ticks(
            Arc::clone(&finished),
            Arc::clone(&ticking_modules),
        ));

        Self {
            modules,
            finished_or_mod_registered,
            is_finished,
            executor: Some(executor),
            finished,
            ticker: Some(ticker),
            ticking_modules,
        }
    }

    // Main system's task, which lets modules handle their messages.
    async fn execute(
        added: Modules,
        finished_or_mod_registered: Arc<Notify>,
        is_finished: Arc<AtomicBool>,
    ) {
        let mut executors = Vec::new();
        let (tx, rx) = unbounded();

        loop {
            finished_or_mod_registered.notified().await;
            if is_finished.load(Ordering::Relaxed) {
                Self::finished(tx, executors).await;
                break;
            } else {
                Self::mod_registered(&mut executors, &added, &rx).await;
            }
        }
    }

    async fn mod_registered(
        executors: &mut Vec<JoinHandle<()>>,
        added: &Modules,
        rx: &Receiver<()>,
    ) {
        let mut new_modules = added.lock().await;
        executors.extend(
            new_modules
                .drain(..)
                .map(|m| tokio::spawn(m.execute(rx.clone()))),
        );
    }

    async fn finished(tx: Sender<()>, executors: Vec<JoinHandle<()>>) {
        let cnt = tx.receiver_count();
        for _ in 1..cnt {
            // 1 receiver is for cloning in Self::execute()
            tx.send(())
                .await
                .expect("Executor should not have been dropped");
        }

        for exe in executors {
            if let Err(err) = exe.await {
                error!("A module executor failed joining, reason: {err}");
                if err.is_panic() {
                    panic!("A module executor panicked!");
                }
            }
        }
    }

    // Task, which handles shutting down sending ticks to modules.
    async fn shutdown_ticks(finished: Arc<Notify>, ticking_modules: Arc<Mutex<Vec<Sender<()>>>>) {
        // Shutdown ticking modules.
        finished.notified().await;
        let mut ticking_modules = ticking_modules.lock().await;
        for tx in ticking_modules.drain(..) {
            let _ = tx.send(()).await;
        }
    }

    /// Gracefully shut the system down.
    pub async fn shutdown(&mut self) {
        self.is_finished.store(true, Ordering::Relaxed);
        self.finished_or_mod_registered.notify_one();
        let executor = self
            .executor
            .take()
            .expect("Shutting down already shut down system");
        self.finished.notify_one();
        let ticker = self
            .ticker
            .take()
            .expect("Shutting down already shut down system");

        if let Err(err) = executor.await {
            error!("System's main task failed joining, reason: {err}");
            if err.is_panic() {
                panic!("A module executor panicked!");
            }
        }

        if let Err(err) = ticker.await {
            error!("System's ticker task failed joining, reason: {err}");
        }

        // Drop modules which were not run yet.
        // An already run module `m` is going to get dropped
        // once `m.execute(finished_rx)` finishes.
        self.drop_modules().await;
    }

    async fn drop_modules(&mut self) {
        let mut modules = self.modules.lock().await;
        modules.clear();
    }
}

/// A reference to a module used for sending messages.
pub struct ModuleRef<T: Module + ?Sized> {
    tx: Sender<Box<dyn Handlee<T>>>,
    ticking_modules: Arc<Mutex<Vec<Sender<()>>>>,
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
        let self_clone = self.clone();
        tokio::spawn(self_clone.tick(message, delay, rx));
        let mut ticking_modules = self.ticking_modules.lock().await;
        ticking_modules.push(tx.clone());
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
                    f.expect("Finishing channel shouldn't close (System should have a reference)");
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
            ticking_modules: Arc::clone(&self.ticking_modules),
        }
    }
}

struct ModuleExecutor<T: Module> {
    module_ref: ModuleRef<T>,
    module: T,
    rx: Receiver<Box<dyn Handlee<T>>>,
}

#[async_trait::async_trait]
impl<T: Module> Mod for ModuleExecutor<T> {
    async fn execute(mut self: Box<Self>, finished: Receiver<()>) {
        loop {
            tokio::select! {
                biased;

                f = finished.recv() => {
                    f.expect("Finishing channel shouldn't close");
                    break;
                }
                msg = self.rx.recv() => {
                    let msg = msg
                        .expect("All ModuleRefs being dropped shouldn't be possible (Mod should own one)");
                    msg.get_handled(&self.module_ref, &mut self.module).await;
                }
            };
        }
    }
}
