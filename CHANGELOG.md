# Changelog

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
