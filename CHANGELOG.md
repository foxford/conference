# Changelog

## v0.6.17 (May 20, 2021)

### Changes
- Allow agent connections to multiple RTCs ([14b06b6](https://github.com/netology-group/conference/commit/14b06b69a703f410bff9c31a7831d3110eb83796))
- Consider N^2 load for owned RTC sharing policy & writer bitrate ([676d2c1](https://github.com/netology-group/conference/commit/676d2c141b584c34583a1aede2d1570949ebb7f6))
- Change authz object for agent writer config ([f94be0c](https://github.com/netology-group/conference/commit/f94be0c884ccaf365e4e62f99d6a2aa45bf48180))
- Move max room duration into config ([ca721bb](https://github.com/netology-group/conference/commit/ca721bbc4ce992c144f1e6008e2f9d83b1e10784))
- Remove room audience update option ([2477c88](https://github.com/netology-group/conference/commit/2477c88e5caf7bdbc3e269b9f5c1e26990490d22))

### Fixes
- Delete only connections related to the disconnected stream's RTC ([b0fb07f](https://github.com/netology-group/conference/commit/b0fb07f083059f5fcd3e984b982494c3b531b68b))

## v0.6.16 (May 19, 2021)

### Changes
- Remove redundant authz objects, fix room update ([37038db](https://github.com/netology-group/conference/commit/37038db3cfcfc529339e872fc3ac4382f6bfd2ef))

## v0.6.15 (May 12, 2021)

### Features
- Add handle id validation ([2607305](https://github.com/netology-group/conference/commit/260730592a4fb04a9fe814220ff1475b737eb762))
- Randomize ordering in balancer queries ([7f07691](https://github.com/netology-group/conference/commit/7f07691414646111768be32385a9cc02d95bc4cd))

### Changes
- Rename class_id to classroom_id ([b70c9bf](https://github.com/netology-group/conference/commit/b70c9bfe6a7b354b27a6b16861adf8b344ba4039))

## v0.6.14 (April 23, 2021)

### Features
- Add rtc.created_by to room.upload event ([6b4afcd](https://github.com/netology-group/conference/commit/6b4afcd0c01dbfab95176d1307e30941a93c5c35))
- Separate upload configs for sharing policies ([611c384](https://github.com/netology-group/conference/commit/611c3845313771d17bd63a2ac3592d5726fd5dd3))
- Add class_id to room ([8a0cf82](https://github.com/netology-group/conference/commit/8a0cf8226754f6bbeeade5b87cb649fffc9719a9))

### Changes
- Remove room.delete endpoint ([6b50b3b](https://github.com/netology-group/conference/commit/6b50b3bceb7d54fd43163a7fc6246ff91398654c))

### Fixes
- Fix inserting recording for the second RTC ([b61d33c](https://github.com/netology-group/conference/commit/b61d33c07fbb9d416b7647798351f9e6dc9e7673))


## v0.6.13 (April 1, 2021)

### Features
- Add agent reader & writer config endpoints ([256bd96](https://github.com/netology-group/conference/commit/256bd96db6754a6250044eb31ce0435a8146b266), [05c2c33](https://github.com/netology-group/conference/commit/05c2c330aae9b87dcdb01796d46c133700db5d07))

### Changes
- Increase max webinar duration to 7 hours ([736e16b](https://github.com/netology-group/conference/commit/736e16bbb61839f3aa75a1044d05330d4e592be4))


## v0.6.12 (March 17, 2021)

### Changes
- Moved to new dynsub scheme ([b7ceab9](https://github.com/netology-group/conference/commit/b7ceab9c45714447faf25140e1b5e3cc71025ae7))
- Added RTC sharing policy and created_by ([ec9d72d](https://github.com/netology-group/conference/commit/ec9d72d20b632eba825c7347973958ffea4b1eb7))

## v0.6.11 (February 9, 2021)

### Fixes
- Vacuum only rooms on compliant backends ([2f53692](https://github.com/netology-group/conference/commit/2f536925e88935eef7c4535eedc14e5e33cd2cb2))


## v0.6.10 (February 2, 2021)

### Features
- Restrict working only with compliant janus API version ([3534b64](https://github.com/netology-group/conference/commit/3534b6474c743f268dfdd6403a2bc7dfa90da54e))


## v0.6.9 (February 1, 2021)

### Features
- Refactor JSEP validation & allow lists ([e1784b3](https://github.com/netology-group/conference/commit/e1784b36c1a357a5868e974ac65392ad2c933dad))


## v0.6.8 (January 28, 2021)

### Features
- Allow unbounded rooms ([477d78d](https://github.com/netology-group/conference/commit/477d78df0f93b2b79b6537c463959a4f707a59ab))
- Replace connected agent status with agent connection ([c1a7058](https://github.com/netology-group/conference/commit/c1a7058a695258430171676ecad4518a55472f35))

### Fixes
- Fix bulk disconnect ([7338a91](https://github.com/netology-group/conference/commit/7338a91858f05e4e850b4c134882a0f93fd99482))


## v0.6.7 (January 28, 2021)

### Fixes
- Fixed JanusTimeoutsTotal name ([a4fd512](https://github.com/netology-group/conference/commit/a4fd5127ecc5d4e09a3f8214d48d84ae15c02b66))


## v0.6.6 (January 12, 2021)

### Changes
- Allow users to exceed room reserves ([a5aea1c](https://github.com/netology-group/conference/commit/a5aea1c04044a87c51e698d6455de05e3f601c53))
- Added running requests durations metrics ([395c4fb](https://github.com/netology-group/conference/commit/395c4fbe46a6606efdbfca91fd18f6624fc0e8bf))


## v0.6.5 (December 16, 2020)

### Changes
- Added unique index on rtc.room_id ([da24b7d](https://github.com/netology-group/conference/commit/da24b7df0b04740d1e8135b4583d9e3cf30a5f9e))
- Added janus timeouts metric ([e610a03](https://github.com/netology-group/conference/commit/e610a03ad03754314d2217c423b502d2231b1ec5))

### Fixes
- Fixed unwrap crashing metrics aggregator ([7ef6fb3](https://github.com/netology-group/conference/commit/7ef6fb3ab1ce1dad839a322dcb1af5e8be89b6be))


## v0.6.4 (December 8, 2020)

### Changes
- svc-error update ([f2dc52d](https://github.com/netology-group/conference/commit/f2dc52da777565ad8fdbf1adabe54a1497380988))
- Moved backend binding to room ([1c27500](https://github.com/netology-group/conference/commit/1c275009ddb9fac0efd9d39e7885e0a708a19b21))
- Allowed janus to return state=already_running to work around vacuum overlaps ([3a58024](https://github.com/netology-group/conference/commit/3a5802476853575fc28cff669ef9ca83993064e8))
- Set unbounded rooms closure timeout to 6 hours ([1017e3f](https://github.com/netology-group/conference/commit/1017e3f5c26ab29b35417d5c5abafc2322237829))
- svc-agent update to v0.14.12 ([a9c6df0](https://github.com/netology-group/conference/commit/a9c6df05c54781b3a411310f59022f46e032b6a9))

### Fixes
- Removed unicast subscription, changed shared unicast janus responses subscription to non-shared ([2fc263b](https://github.com/netology-group/conference/commit/2fc263b01808c01db2915e2e073ef671bad1ed45))
- Fixed slog-envlogger initialization ([a56ad3d](https://github.com/netology-group/conference/commit/a56ad3d0e6b1d24f0d5820db16570a68aaf85239))
- Fixed pg types in load_for_each_backend query ([d133a26](https://github.com/netology-group/conference/commit/d133a262b84f6f67495ac106a0cf1c4bea0222ca))


## v0.6.3 (November 11, 2020)

### Changes
- Changed rtc list to sort by created_at asc ([02e6b48](https://github.com/netology-group/conference/commit/02e6b48624903ac181788c4da10e9f9c72719762))

### Fixes
- Reverted svc-agent update to 0.14.11, which bloats memory consumption ([6a08c34](https://github.com/netology-group/conference/commit/6a08c3457516bac6fb6bfaf012747a1b18c2a4cb))

## v0.6.2 (October 26, 2020)

### Changes
- Added audience level notification of room closure ([67a3fb1](https://github.com/netology-group/conference/commit/67a3fb18a3fde0ecb6565ddb508367262dcdddad))

### Fixes
- Fixed mqtt metrics labels ([1e692c1](https://github.com/netology-group/conference/commit/1e692c120edc2e6505713a33d16842fb36cc277a))

## v0.6.1 (October 23, 2020)

### Changes
- Added connected agents load for each backend ([ba0567d](https://github.com/netology-group/conference/commit/ba0567dc72df735d2be899995c74e48b48921259))

### Fixes
- Fixed mqtt metrics ([3920516](https://github.com/netology-group/conference/commit/3920516d2b2fc611b25eb3979b68deeb97842baa))

## v0.6.0 (October 20, 2020)

### Changes
- Send backend and bucket from config to stream.upload ([8cc41bf](https://github.com/netology-group/conference/commit/8cc41bf7e449c14b1dce1f6519949746b0e97a56))
- Add selective sentry notification ([19302cd](https://github.com/netology-group/conference/commit/19302cd9ea9744b8a33511882264a10e3b7ced9f))

### Fixes
- Fix type mismatch and missing backends in reserve_load_for_each_backend query ([3d54297](https://github.com/netology-group/conference/commit/3d5429770f95d81b48df9a6d70b6ddfcf6f18322))


## v0.5.15 (October 16, 2020)

### Changes
- Added StatsRoute for metrics ([b2f1393](https://github.com/netology-group/conference/commit/b2f139394503b1cbf6d4ffdedadbe8280e7c2dcc)


## v0.5.14 (October 15, 2020)

### Fixes
- Fixed room query in UploadStream janus transaction ([e3c5fc3](https://github.com/netology-group/conference/commit/e3c5fc378ce31f8dc80136b72782ce7913959ccd)
- Added time validation for room update ([fe48c6e](https://github.com/netology-group/conference/commit/fe48c6e2583689c97f3c5371b4266213e9d1b050)


## v0.5.13 (October 12, 2020)

### Changes
- Added typed errors ([0c6946e](https://github.com/netology-group/conference/commit/0c6946eef167a5823a9c667341b9f638fcce8af7)
- Separated missing from closed room errors ([cb6c9ae](https://github.com/netology-group/conference/commit/cb6c9ae68c026ddeadb2d363aee915af6c0ce8f7)

### Fixes
- Fixed ordering in least loaded query ([ccf06a9](https://github.com/netology-group/conference/commit/ccf06a97ffe495f8d1ddacea9603648fefcf7df2)
- Fixed free capacity query not allowing users to connect even if there are free slots ([6795504](https://github.com/netology-group/conference/commit/67955045d3c672838fa9e6233cf7714e403fcb37)


## v0.5.12 (October 8, 2020)

### Features
- Add falling back to the least loaded backend ([f797bbf](https://github.com/netology-group/conference/commit/f797bbf243f23f4ba9f183425ed26296ef7ab24a))
- Add logger tags ([e052bd1](https://github.com/netology-group/conference/commit/e052bd18dbe83ae0535a2903fc0c4b95de6cbb90))

### Changes
- Replace janus_rtc_stream with recording in balancer queries ([5e604b3](https://github.com/netology-group/conference/commit/5e604b375e782fbe986f0fdf2b7d83f92e144bfe))

### Fixes
- Minor fixes ([7a47280](https://github.com/netology-group/conference/commit/7a472803cdd0c021addcfe9c81f5ea5d40a8eac3), [f2b841a](https://github.com/netology-group/conference/commit/f2b841a2ec2fb539a61fcf2cfb804092abc45caf))


## v0.5.11 (October 6, 2020)

### Changes
- Change balancing method to greedy ([f311681](https://github.com/netology-group/conference/commit/f31168121e3c9d7626b7f345e39823ce6cb0de56))


## v0.5.10 (October 6, 2020)

### Features
- Add balancer capacity ([2b601a9](https://github.com/netology-group/conference/commit/2b601a95ab02ed96b3c96accf6e9f92534b6d958))
- Add janus request timeout ([183eb7a](https://github.com/netology-group/conference/commit/183eb7a9bb32c9f7e0f8cf4cbfb6d62c03c3bbe7))
- Add room.close notification ([cc1af9a](https://github.com/netology-group/conference/commit/cc1af9a6ab8c609b29f32996e2a6c9014bbd60ac))


## v0.5.9 (September 29, 2020)

### Features
- Send rtc_stream.update when the backend goes offline ([0326e1d](https://github.com/netology-group/conference/commit/0326e1d18923638b83ff39e27037da6288626afe), [7a0378c](https://github.com/netology-group/conference/commit/7a0378c7a4e4460b70b3d3be84a56cd58eed3fd3))
- Add tags to rooms ([87590ba](https://github.com/netology-group/conference/commit/87590baa9e0170805108b517ca26af76343a6464))


## v0.5.8 (September 22, 2020)

### Changes
- Relate recordings to backends w/o mediation of streams ([dba75e2](https://github.com/netology-group/conference/commit/dba75e2fb0591a25713a368d9ad2044b6bead074), [d1fe7a4](https://github.com/netology-group/conference/commit/d1fe7a41345dbaf165f436abd14f752a51643ed6), [83dcbb5](https://github.com/netology-group/conference/commit/83dcbb5817a48d51daf23b1e30a6e728a465690b))


## v0.5.7 (September 16, 2020)

### Features
- Add metric2 with different serialization tag ([b24105a](https://github.com/netology-group/conference/commit/b24105aac775daee675b2103a9b81b3e89ac15ad))


## v0.5.6 (September 9, 2020)

### Fixes
- Fix dynamic metrics serialization ([5aca16c](https://github.com/netology-group/conference/commit/5aca16c3718ccdefbb9836cfc8ea7d63f4a7dfdb))


## v0.5.5 (September 9, 2020)

### Features
- Updated svc agent, allows to tune threads configuration in tokio and async_std runtimes ([f890119](https://github.com/netology-group/conference/commit/f8901198f348845afe6e6978ed306fbdc08c9e0c))
- Added label metrics for message.broadcast ([9218acc](https://github.com/netology-group/conference/commit/9218accf25abbafc4952211ea7a4e5befe9982f6))
- Added janus and connected agents metrics ([dee2d34](https://github.com/netology-group/conference/commit/dee2d34cf0a52d2da5c7cdc0c5dd80e4e0af65b0))


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
