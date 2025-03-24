// Copyright (c) 2017, All Contributors (see CONTRIBUTORS file)
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

use super::super::messaging;
use super::{
    offset_by_size, Dispatcher, Env, EnvId, Error, PassResult, TryInstruction, ERROR_EMPTY_STACK,
};

use std::marker::PhantomData;

instruction!(PUBLISH, (a, b => ), b"\x87PUBLISH");
instruction!(SUBSCRIBE, (a => ), b"\x89SUBSCRIBE");
instruction!(UNSUBSCRIBE, (a => ), b"\x8bUNSUBSCRIBE");

pub struct Handler<'a, P: messaging::Publisher, S: messaging::Subscriber> {
    publisher: P,
    subscriber: S,
    phantom: PhantomData<&'a ()>,
}

impl<'a, P: messaging::Publisher, S: messaging::Subscriber> Dispatcher<'a> for Handler<'a, P, S> {
    fn handle(&mut self, env: &mut Env<'a>, instruction: &[u8], pid: EnvId) -> PassResult<'a> {
        self.handle_publish(env, instruction, pid)
            .if_unhandled_try(|| self.handle_subscribe(env, instruction, pid))
            .if_unhandled_try(|| self.handle_unsubscribe(env, instruction, pid))
            .if_unhandled_try(|| Err(Error::UnknownInstruction))
    }
}

impl<'a, P: messaging::Publisher, S: messaging::Subscriber> Handler<'a, P, S> {
    pub fn new(publisher: P, subscriber: S) -> Self {
        Handler {
            publisher,
            subscriber,
            phantom: PhantomData,
        }
    }

    #[inline]
    fn handle_publish(
        &mut self,
        env: &mut Env<'a>,
        instruction: &[u8],
        _: EnvId,
    ) -> PassResult<'a> {
        return_unless_instructions_equal!(instruction, PUBLISH);
        let topic = env.pop().ok_or_else(|| error_empty_stack!())?;
        let data = env.pop().ok_or_else(|| error_empty_stack!())?;

        self.publisher.publish(topic, data);

        Ok(())
    }

    #[inline]
    fn handle_subscribe(
        &mut self,
        env: &mut Env<'a>,
        instruction: &[u8],
        _: EnvId,
    ) -> PassResult<'a> {
        return_unless_instructions_equal!(instruction, SUBSCRIBE);

        let topic = env.pop().ok_or_else(|| error_empty_stack!())?;

        match env.published_message_callback() {
            None => (),
            Some(cb) => {
                let ident = self.subscriber.subscribe(topic, cb);
                let slice = alloc_and_write!(&ident, env);
                env.push(slice);
            }
        }

        Ok(())
    }

    #[inline]
    fn handle_unsubscribe(
        &mut self,
        env: &mut Env<'a>,
        instruction: &[u8],
        _: EnvId,
    ) -> PassResult<'a> {
        return_unless_instructions_equal!(instruction, UNSUBSCRIBE);

        let identifier = env.pop().ok_or_else(|| error_empty_stack!())?;

        self.subscriber.unsubscribe(identifier);

        Ok(())
    }
}

#[cfg(test)]
#[allow(unused_variables, unused_must_use, unused_mut)]
mod tests {

    use crate::messaging;
    use crate::nvmem::MmapedFile;
    use crate::script::{dispatcher, Env, EnvId, Error, ResponseMessage, Scheduler};
    use crate::storage;
    use crate::timestamp;
    use crossbeam;
    use lmdb;
    use pumpkinscript::parse;
    use std::fs;
    use std::sync::mpsc;
    use std::sync::Arc;
    use tempdir::TempDir;

    use std::time::Duration;

    #[test]
    fn subscribe_publish() {
        let (sender0, receiver0) = mpsc::channel();
        eval!(
            "\"Topic\" SUBSCRIBE \"Hello\" \"Topic\" PUBLISH",
            env,
            result,
            sender0.clone(),
            receiver0,
            {
                assert!(result.is_ok());

                let result = receiver0.recv_timeout(Duration::from_secs(1)).unwrap();
                assert_eq!(result, (Vec::from("Topic"), Vec::from("Hello")));
            }
        );

        eval!(
            "\"Hello\" \"Topic1\" PUBLISH",
            env,
            result,
            sender0.clone(),
            receiver0,
            {
                assert!(result.is_ok());
                assert!(receiver0.recv_timeout(Duration::from_secs(1)).is_err());
            }
        );
    }

    #[test]
    fn unsubscribe() {
        let (sender0, receiver0) = mpsc::channel();

        eval!(
            "\"Topic\" SUBSCRIBE UNSUBSCRIBE \"Hello\" \"Topic\" PUBLISH",
            env,
            result,
            sender0.clone(),
            receiver0,
            {
                assert!(result.is_ok());
                assert!(receiver0.recv_timeout(Duration::from_secs(1)).is_err());
            }
        );
    }
}
