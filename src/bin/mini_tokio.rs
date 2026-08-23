// src/bin/mini_tokio.rs

// Import std::task::Context and Poll separately
use std::task::{Context, Poll};
use std::thread;

// Import futures::task which contains ArcWake
use futures::task::ArcWake;
use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A structure holding a future and the result of the latest call to its `poll` method.
struct TaskFuture {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
    poll: Poll<()>,
}

/// The Task harness. It holds the future and knows how to schedule itself.
struct Task {
    task_future: Mutex<TaskFuture>,
    executor: mpsc::Sender<Arc<Task>>,
}

impl Task {
    /// Schedule the task by sending it to the executor channel.
    fn schedule(self: &Arc<Self>) {
        self.executor.send(self.clone());
    }
}

impl ArcWake for Task {
    /// Called when this task's waker is woken up.
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.schedule();
    }
}

impl TaskFuture {
    fn new(future: impl Future<Output = ()> + Send + 'static) -> TaskFuture {
        TaskFuture {
            future: Box::pin(future),
            poll: Poll::Pending,
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) {
        // Spurious wake-ups are allowed, even after a future has returned `Ready`.
        // However, polling a future which has already returned `Ready` is *not* allowed.
        if self.poll.is_pending() {
            self.poll = self.future.as_mut().poll(cx);
        }
    }
}

impl Task {
    /// Polls the inner future.
    fn poll(self: Arc<Self>) {
        // Create a waker from the Task instance using ArcWake
        // We use futures::task::waker
        let waker = futures::task::waker(self.clone());
        let mut cx = Context::from_waker(&waker);

        // Get exclusive access to the task_future
        let mut task_future = self.task_future.try_lock().unwrap();

        // Poll the inner future
        task_future.poll(&mut cx);
    }

    /// Spawns a new task with the given future.
    fn spawn<F>(future: F, sender: &mpsc::Sender<Arc<Task>>)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Arc::new(Task {
            task_future: Mutex::new(TaskFuture::new(future)),
            executor: sender.clone(),
        });

        let _ = sender.send(task);
    }
}

struct MiniTokio {
    scheduled: mpsc::Receiver<Arc<Task>>,
    sender: mpsc::Sender<Arc<Task>>,
}

impl MiniTokio {
    fn new() -> MiniTokio {
        let (sender, scheduled) = mpsc::channel();
        MiniTokio { scheduled, sender }
    }

    /// Spawn a future onto the mini-tokio instance.
    fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Task::spawn(future, &self.sender);
    }

    /// Run the executor until all tasks are complete.
    fn run(&self) {
        while let Ok(task) = self.scheduled.recv() {
            task.poll();
        }
    }
}

#[tokio::main]
async fn main() {
    let mini_tokio = MiniTokio::new();

    // Spawn a task that will delay for 10ms
    mini_tokio.spawn(async {
        let when = Instant::now() + Duration::from_millis(10);
        println!("Task started at {:?}", when);

        let out = Delay { when }.await;
        println!("Task finished! Output: {}", out);
    });

    println!("Starting Mini Tokio executor...");
    mini_tokio.run();
    println!("Mini Tokio executor finished.");
}

/// A simple Future that waits until a specific instant.
struct Delay {
    when: Instant,
}

impl Future for Delay {
    type Output = &'static str;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<&'static str> {
        if Instant::now() >= self.when {
            println!("Delay finished!");
            Poll::Ready("done")
        } else {
            // Get a handle to the waker for the current task
            let waker = cx.waker().clone();
            let when = self.when;

            // Spawn a timer thread.
            thread::spawn(move || {
                let now = Instant::now();

                if now < when {
                    thread::sleep(when - now);
                }

                waker.wake();
            });

            Poll::Pending
        }
    }
}