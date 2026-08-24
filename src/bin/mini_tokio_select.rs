// src/bin/mini_tokio_select.rs

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use rand::seq::SliceRandom;

/// A task that can be scheduled.
pub struct Task {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
    sender: mpsc::Sender<Arc<Task>>,
}

impl Task {
    pub fn new(
        future: impl Future<Output = ()> + Send + 'static,
        sender: mpsc::Sender<Arc<Task>>,
    ) -> Self {
        Task {
            future: Box::pin(future),
            sender,
        }
    }

    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.future.as_mut().poll(cx)
    }
}

/// A small custom Select future.
///
/// It polls several futures and completes when the first one
/// returns Poll::Ready.
pub struct MySelect {
    branches: Vec<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl MySelect {
    pub fn new(
        branches: Vec<Pin<Box<dyn Future<Output = ()> + Send>>>,
    ) -> Self {
        Self { branches }
    }
}

impl Future for MySelect {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // FIXED: Use rand::rng() instead of rand::thread_rng() for rand 0.10+
        let mut rng = rand::rng();
        self.branches.shuffle(&mut rng);

        for branch in &mut self.branches {
            match branch.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    println!("A branch completed first.");
                    return Poll::Ready(());
                }
                Poll::Pending => {}
            }
        }

        Poll::Pending
    }
}

/// A simple timer future.
///
/// It becomes ready after `duration` has elapsed.
pub struct Delay {
    when: Instant,
}

impl Delay {
    pub fn new(duration: Duration) -> Self {
        Self {
            when: Instant::now() + duration,
        }
    }
}

impl Future for Delay {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if Instant::now() >= self.when {
            Poll::Ready(())
        } else {
            // Wake the executor after a short delay so the future
            // gets polled again.
            let waker = cx.waker().clone();

            thread::spawn(move || {
                thread::sleep(Duration::from_millis(10));
                waker.wake();
            });

            Poll::Pending
        }
    }
}

/// A minimal executor used to demonstrate our custom Select future.
fn block_on<F>(mut future: F) -> F::Output
where
    F: Future + Unpin,
{
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    loop {
        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::sleep(Duration::from_millis(1)),
        }
    }
}

fn main() {
    println!("Starting mini select demo...");

    let first = Box::pin(Delay::new(Duration::from_millis(500)));
    let second = Box::pin(Delay::new(Duration::from_millis(150)));
    let third = Box::pin(Delay::new(Duration::from_millis(300)));

    let select = MySelect::new(vec![first, second, third]);

    let start = Instant::now();

    block_on(select);

    println!(
        "Select completed after approximately {:?}.",
        start.elapsed()
    );
}