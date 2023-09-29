use std::vec::IntoIter;

use futures::{Future, stream, StreamExt};
use futures::stream::{BufferUnordered, Iter};
use tokio::runtime::{Builder, Runtime};

pub(crate) struct BoundedIter<F>
where
    F: Future,
{
    iter: BufferUnordered<Iter<IntoIter<F>>>,
    rt: Runtime
}

impl<F> BoundedIter<F>
where
    F: Future,
{
    pub(crate) fn from_vec(vec: Vec<F>, limit: usize) -> Self {
        let stream = stream::iter(vec);
        BoundedIter {
            iter: stream.buffer_unordered(limit),
            rt: Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
        }
    }
}

impl<F> Iterator for BoundedIter<F>
where
    F: Future,
{
    type Item = F::Output;

    fn next(&mut self) -> Option<Self::Item> {
        self.rt.block_on(&mut self.iter.next())
    }
}
