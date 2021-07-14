pub mod watch_progress {
    use std::pin::Pin;
    use std::sync::{Arc, Weak};
    use std::task::{Context, Poll};

    use futures::task::AtomicWaker;
    use futures::Stream;

    pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
        let shared = Arc::new(parking_lot::Mutex::new(State {
            waker: AtomicWaker::new(),
            value: None,
            closed: false,
        }));
        let tx = Sender { shared: Arc::downgrade(&shared) };
        let rx = Receiver { shared };
        (tx, rx)
    }

    #[derive(Debug)]
    pub struct Receiver<T> {
        shared: Arc<parking_lot::Mutex<State<T>>>,
    }

    impl<T> Receiver<T> {
        #[allow(dead_code)]
        pub fn is_closed(&self) -> bool {
            self.shared.lock().closed
        }
    }

    #[derive(Debug)]
    pub struct Sender<T> {
        shared: Weak<parking_lot::Mutex<State<T>>>,
    }

    impl<T> Sender<T> {
        #[allow(dead_code)]
        pub fn has_receiver(&self) -> bool {
            self.shared.strong_count() > 0
        }

        #[allow(dead_code)]
        pub fn is_closed(&self) -> bool {
            match self.shared.upgrade() {
                Some(shared) => shared.lock().closed,
                None => true,
            }
        }

        #[allow(dead_code)]
        pub fn close(&mut self) {
            if let Some(shared) = self.shared.upgrade() {
                let mut data = shared.lock();
                data.closed = true;
            }
        }

        pub fn send_acc(&mut self, f: impl FnOnce(Option<T>) -> T) -> bool {
            if let Some(shared) = self.shared.upgrade() {
                let mut data = shared.lock();
                if !data.closed {
                    data.value = Some(f(data.value.take()));
                    data.waker.wake();
                    return true;
                }
            }
            false
        }

        #[allow(dead_code)]
        pub fn send(&mut self, msg: T) -> bool {
            if let Some(shared) = self.shared.upgrade() {
                let mut data = shared.lock();
                if !data.closed {
                    data.value = Some(msg);
                    data.waker.wake();
                    return true;
                }
            }
            false
        }
    }

    impl<T> Drop for Sender<T> {
        fn drop(&mut self) {
            if let Some(shared) = self.shared.upgrade() {
                let mut data = shared.lock();
                data.closed = true;
                data.waker.wake();
            }
        }
    }

    #[derive(Debug)]
    struct State<T> {
        waker: AtomicWaker,
        value: Option<T>,
        closed: bool,
    }

    impl<T> Stream for Receiver<T> {
        type Item = T;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let mut data = self.shared.lock();
            match data.value.take() {
                Some(res) => Poll::Ready(Some(res)),
                None => {
                    if data.closed {
                        Poll::Ready(None)
                    } else {
                        data.waker.register(cx.waker());
                        Poll::Pending
                    }
                }
            }
        }
    }
}
