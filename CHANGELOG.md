# Changelog

## v0.5.4 (August 31, 2020)

### Features
- Add DB pool metrics ([91ad494](https://github.com/netology-group/conference/commit/91ad494d339603031b114cebdac1bca4286966e3), [f4c695a](https://github.com/netology-group/conference/commit/f4c695ae08c4bcf270a73b308d04317f9718c23a))

### Fixes
- Add in_progress status for recordings and prevent stream cascade deletion ([084d03f](https://github.com/netology-group/conference/commit/084d03fb6280905f80a831ad595f9ccc23464d10))


## v0.5.3 (August 27, 2020)

### Features
- Add redis_connections_total metric ([b40776c](https://github.com/netology-group/conference/commit/b40776cba32fbf2d687c2dd0de2e16d0d95c4245))
- Transmit idle connections metrics both for redis and pg pools ([59c36db](https://github.com/netology-group/conference/commit/59c36db8b31a506e553796154f22165f62463824))


## v0.5.2 (August 26, 2020)

### Features
- Consider reserve in stream pauses ([3e262f0](https://github.com/netology-group/conference/commit/3e262f02bc7c77c3d44a8948ab4c4fefb34bd2dc))


## v0.5.1 (August 22, 2020)

### Fixes
- Fix considering reserve on load count for readers ([181fc05](https://github.com/netology-group/conference/commit/181fc05b2210a39cf00497765e18a029f85ec08d))
- Fix duplicate rooms in finished_without_recordings query ([90ef8ff](https://github.com/netology-group/conference/commit/90ef8ffe9e71ba233c7e6b9ba43b2a1f0f251f5e))
- Skip finished rooms without recording and no corresponding rtc_stream ([d2934ec](https://github.com/netology-group/conference/commit/d2934ecc562a2c84bbc19ad827a00f1a82b7da97))
- Put connected agents to ready state on backend disconnect ([11c7679](https://github.com/netology-group/conference/commit/11c76790c7563ffb888aa6562aa71d91a62a1fa8))
- Update balancer test ([78c1e42](https://github.com/netology-group/conference/commit/78c1e42a16872056af08a106ece333a3e1bf1d3b))
- Put readers to ready state on stream stop ([1c8854d](https://github.com/netology-group/conference/commit/1c8854d4ca3c887d44c37f8f97ad4ee9c5126494))
- Delete agents of closed rooms on vacuum ([8797737](https://github.com/netology-group/conference/commit/87977371d9ed3dfdc85b6357524655ebada47038))
- Fix least_loaded and agent_count queries to account only for active streams ([f560d45](https://github.com/netology-group/conference/commit/f560d4502223fc02a1e7de5b50fc402631a00eb4))


## v0.5.0 (August 20, 2020)

### Features
- Add intent parameter to rtc.connect ([2fdc33d](https://github.com/netology-group/conference/commit/2fdc33d62f01f5301dcc11278b4c927c10fb4238))

### Changes
- Reserve and limit overhaul ([11808eb](https://github.com/netology-group/conference/commit/11808eb1aa9eaed00e24a7961feca3aef0d5b910))
- Handle both hangup and detach events ([16d8537](https://github.com/netology-group/conference/commit/16d8537516757d2c2f29529411cd1fdad205a180))
- Revert publisher migration ([8c3e29b](https://github.com/netology-group/conference/commit/8c3e29b4ee80088d827b9d753dd00d530c0aedfc))
- Add CACHE_ENABLED envvar check ([be8b163](https://github.com/netology-group/conference/commit/be8b163e980437c73d97dfa2e28cb2e6f8315748))
- Subscribe to unicast requests without shared group ([4bbf30a](https://github.com/netology-group/conference/commit/4bbf30aaf4b774a85974e06d959a6f3a4a9e9620))
- Remove deprecated time field in room.upload ([6d3b0b7](https://github.com/netology-group/conference/commit/6d3b0b7f009b3321276ed7d39c8f82d37d7aa1c2))

### Fixes
- Fix publisher migration to another backend ([843f4f6](https://github.com/netology-group/conference/commit/843f4f697f839e0dc38e43f8fe605f3cf77e7d07))
- Fix subscribers limit check ([18b334e](https://github.com/netology-group/conference/commit/18b334e278f6a061c156447c00cf8bff15026a02))
- Fix capacity edge case ([0bb614a](https://github.com/netology-group/conference/commit/0bb614aa603809b1af7f7c42787e3157f8020295))
- Fix vacuum with multiple backends ([e6a577a](https://github.com/netology-group/conference/commit/e6a577a89f4bca944cdf53df8dd0f7844ed9342b))
- Fix connected status toggle & agents count ([e971df1](https://github.com/netology-group/conference/commit/e971df197d3168244b7f5db023c875bb6a505f14))


## v0.4.1 (July 30, 2020)

### Changes
- Added unix signals handlers ([61a020b](https://github.com/netology-group/conference/commit/61a020bd38a46487a7f7b6e00c7276e7c8717fe6))
- Svc agent update ([b5ff057](https://github.com/netology-group/conference/commit/b5ff057344920729fb18fbb90f10eee1c9112888))

## v0.4.0 (July 15, 2020)

### Features
- Add room subscribers limit ([747678e](https://github.com/netology-group/conference/commit/747678edeaabed7376660a99c06aa233e4cc2fae))
- Add backend subscribers limit ([d161159](https://github.com/netology-group/conference/commit/d161159d3e17a467bbd7eaa2480706a9673b97e2))

### Changes
- Update svc-agent & svc-authn ([454575f](https://github.com/netology-group/conference/commit/454575fbf18518c1d9951e58c3cdda9a27a8a021))
- Change mp4 to webm ([04a56ab](https://github.com/netology-group/conference/commit/04a56abb4d5ad6d266f65b4436d3c3984a0c21db))

### Fixes
- Fix rtc_stream.update event on stream stop ([7e68204](https://github.com/netology-group/conference/commit/7e6820438bf6246d89c5ed6fea567ff8912e2b28))


## v0.3.9 (June 11, 2020)

### Changes
- Updated async-std to lower idle CPU utilization ([2b21bba](https://github.com/netology-group/conference/commit/2b21bbafce342f583fbce9fce20445693dfef799))
- Notification loop thread now has a name ([a0fa253](https://github.com/netology-group/conference/commit/a0fa2532e408281f60587c72a2fd21799b237e9d))
- Removed nacks field from slow link event ([a1a627d](https://github.com/netology-group/conference/commit/a1a627d47273e5d3fe52cfee7d628a92ff5639ee))


## v0.3.8 (June 3, 2020)

### Features
- Added resubscription on reconnect ([e6ab003](https://github.com/netology-group/conference/commit/e6ab0039d3541f7fc3ece6459fc2d30262a76ffd))
- Added queue length metrics ([42979fb](https://github.com/netology-group/conference/commit/42979fba0321b365a74e120bbfe249f7b6a8f14a))

### Changes
- Switched to anyhow ([9f5bc75](https://github.com/netology-group/conference/commit/9f5bc756ff8f4506d1f67303fe4d3162b4669c95))
- Updated svc-agent ([884d356](https://github.com/netology-group/conference/commit/884d356478fda3dc475205b8d615cece56b25842))


## v0.3.7 (May 18, 2020)

### Changes
- Switch runtime to smol ([54d5a9f](https://github.com/netology-group/conference/commit/54d5a9f01d7b837a89fba504ec3d294fe17b22b8))
- Optimize DB connection usage ([2bf9225](https://github.com/netology-group/conference/commit/2bf9225d2adad2492e723ee9c331095501c2b5de))


## v0.3.6 (May 15, 2020)

### Features
- DB connection metrics ([32a9eb0](https://github.com/netology-group/conference/commit/32a9eb0c1aaab49cedaf85d368231747b9a89622))

### Changes
- Refactor message handling ([a2b6a33](https://github.com/netology-group/conference/commit/a2b6a33a054c282d28901814c0f34d5bd5aae70f))


## v0.3.5 (May 12, 2020)

### Changes
- Update svc-authz ([314fd9b]((https://github.com/netology-group/conference/commit/314fd9b4a5d655baab8eb9389313e8481bda54faq))
- Switch to Debian buster ([464d442]((https://github.com/netology-group/conference/commit/464d442d3eb57effb5927782b8011eb988f6f3a6))


## v0.3.4 (May 7, 2020)

### Changes
- Send rtc_stream.update event on publisher detach ([ac2dd1a](https://github.com/netology-group/conference/commit/ac2dd1ad2213914a4e51b52086ef16af49b6ef57))
- Switch to async-std runtime ([4b63b4b](https://github.com/netology-group/conference/commit/4b63b4b3aee42b31ff023e913bb8cd14c83998e4))


## v0.3.3 (April 21, 2020)

### Changes
- Added room time presence constraint ([4faaa13](https://github.com/netology-group/conference/commit/4faaa13f28838e3214c5dbc52f006f5aa6511897))
- Updated svc-authz to v0.10 ([2e4cb13](https://github.com/netology-group/conference/commit/2e4cb13c42021159bb3f822dcb292f1ffe9d608e))
- Changed topic of room update notification ([cfa2bc7](https://github.com/netology-group/conference/commit/cfa2bc7e74f2a9f4a844c694c8f01571b242736d))
- Updated svc-agent to v0.12 ([6680242](https://github.com/netology-group/conference/commit/668024254cc5fc76eeea48e28d343bc5f1d1397f))


## v0.3.2 (March 18, 2020)

### Changes
- Rename recording time to segments ([5bccc43](https://github.com/netology-group/conference/5bccc43b2bfb7a5e61ea2bb84d7ebd3dd733b2fc))
- Allow to specify a number of minimum idle database connections ([f3f6b6f](https://github.com/netology-group/conference/f3f6b6f4409d26004c30427c093bd739add94d13))
- Enable authz cache ([7a57450](https://github.com/netology-group/conference/7a57450fa951c952b3fccabdc900b28a99d02bb6))


## v0.3.1 (January 21, 2020)

### Changes
- Upgrade svc-agent & log outgoing messages ([b6aa6d4](https://github.com/netology-group/conference/b6aa6d49977cd153ae9c32ab00cbabf5bc982f5a))

### Fixes
- Fix topics for stream.upload request ([2ceed50](https://github.com/netology-group/conference/2ceed50705eb0109fb832d687c8f938abd59e9bd))


## v0.3.0 (January 15, 2020)

### Features
– Upgrade integration with Janus using request/response pattern ([c3c0846](https://github.com/netology-group/conference/c3c0846ca0faf9fab7308b668809ba01ee7f6cf7), [fb7b4f3](https://github.com/netology-group/conference/fb7b4f3fb3a0b83838f1aad652096dce65821cda))


## v0.2.0 (December 20, 2019)

### Features
- Call `agent.leave` in Janus on subscription deletion ([8f7f2c6](https://github.com/netology-group/conference/commit/8f7f2c67f98b030c653fad9b80e5fbf78c2ce8cd))
- Upgrade to v2 connection ([6a40ed5](https://github.com/netology-group/conference/commit/6a40ed5c3f0c35b91ca70884957cb8e88a6b5014))
- Switch to explicit API versions ([def43dd](https://github.com/netology-group/conference/commit/def43ddbc1ca008f3966267a7c2362957f2eeea0))
- Add simple Janus balancing ([8d81a72](https://github.com/netology-group/conference/commit/8d81a72cc1b68d6d7ff6cd5556d413d9adcc1ba3))

### Fixes
- Remove active streams on publisher disconnection ([865b814](https://github.com/netology-group/conference/commit/865b814ad4cb3ca3f0ba37d5879be04c4f40706a))


## v0.1.2 (November 21, 2019)

### Features
- Add timing ([ccf7766](https://github.com/netology-group/conference/commit/ccf7766f6566963822aa0ce76237c43472ab8376))
- Add tracking ([b66ebdf](https://github.com/netology-group/conference/commit/b66ebdff7f27c36c8e86c015810a7778a8acef29))
- Make authz asynchronous ([5bd5baa](https://github.com/netology-group/conference/commit/5bd5baa4c8d56a79c2e8fb36555c7179a097dcd8))


## v0.1.1 (October 17, 2019)

### Fixes
- Put agent in progress on double room.enter ([c12c9dc](https://github.com/netology-group/conference/commit/c12c9dcaa3455568e0bdab72fd4cbd742fa0a9a0))
- Add agent status filter for room presence check ([ed072e3](https://github.com/netology-group/conference/commit/ed072e30f04164907063ebc978b356a5319cae50))


## v0.1.0 (October 14, 2019)

Initial release
