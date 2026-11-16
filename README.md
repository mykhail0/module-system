# Module system

This asynchronous library implements a module system supporting arbitrary
user-provided types of modules and messages. This was the first assignment in
a Distributed Systems class at the University of Warsaw. The library's public
API and public tests were provided as part of the assignment.

## Module system specification

The module system interface provides the following functionality:

* Creating and starting new instances of the system (`System::new()`).

* Registering modules in the system (`System::register_module()`). The `Module`
trait specifies bounds that must be satisfied by a module. Registering a module
yields a `ModuleRef`, which can be used then to send messages to the module.

* Sending messages to registered modules (`ModuleRef::send()`). A message of
type `M` can be sent to a module of type `T` if `T` implements the `Handler<M>`
trait. A module handles messages in the order in which it receives them.

* Creating new references to registered modules
(`<ModuleRef as Clone>::clone()`).

* Scheduling a `Tick` message to be sent to a registered module periodically
with a given interval (`ModuleRef::request_tick()`). The first tick is sent
after the interval elapses. Requesting a tick yields a `TimerHandle`, which can
be used to stop the sending of further ticks resulting from this request
(`TimerHandle::stop()`).

  `ModuleRef::request_tick()` can be called multiple times. Every call results
in sending more ticks and does not cancel ticks resulting from previous calls.
For example, if `ModuleRef::request_tick()` is called at time 0 with interval 2
and at time 1 with interval 3, ticks arrive at times 2, 4, 4 (two ticks at time
4), 6, 7, …

* Shutting the system down gracefully (`System::shutdown()`). The shutdown
waits for all already started handlers to finish and for all registered modules
to be dropped. It does not wait for all enqueued messages to be handled. It does
not wait for all Tokio tasks to finish, but it causes all of them to finish (for
example, a task handling `ModuleRef::request_tick()` may finish an interval
after the shutdown).

  It is undefined what happens when the system is used after shutdown. However,
a shutdown will not cause any panics in handlers or Tokio tasks.

The `Message` trait defines what is expected from messages in the module system.
It is also possible to use `ModuleRef` as `Message`.

The file [`public-tests/tests/modules.rs`](public-tests/tests/modules.rs)
contains an example of how the module system can be used. In the example there
are two modules: `Ping` and `Pong`. After being registered in the system, they
are sent references to each other so that they can communicate. Then one of the
modules sends a `Ball` message to the other module. That module replies to it
with a new `Ball` message, to which the first module replies with the next
`Ball` message, and so forth.

### Additional requirements

The library is **asynchronous** and allows multiple modules to execute
**concurrently**. It is designed to be run using Tokio.

Ticks requested by `System::request_tick()` are delivered at specified time
intervals. There is no drift, that is, the difference between the expected
and actual number of ticks sent so far in any moment after
`ModuleRef::request_tick()` is called is bounded.

The module system does not panic unless a registered module panics (because of a
user-provided message handler).

## Message type

There is a question of what type to use for “some message that can be handled by
a module of type `T`”? The library deals with this issue by introducing a
private helper trait and using `async-trait`:

```rust
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
```

Then a trait object of type `Box<dyn Handlee<T>>` is used. The traits `Handler`
and `Handlee` are related in a way similar to `From` and `Into`. An important
difference is that here we use `self: Box<Self>`. Thanks to that when calling
`get_handled()` we can move the box rather than the boxed value. The latter
would be impossible, because in `Box<dyn Handlee<T>>` the boxed value has a
statically unknown size.

## Testing

A subset of official tests is given. Their intention is to make sure that
the public interface of the library is correct, and to evaluate basic
functionality.

Authors: F. Plata, K. Iwanicki, M. Banaszek, W. Ciszewski.
