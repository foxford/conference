# Conference

A WebRTC based conference media server.

## Update sqlx-data.json

```bash
export DATABASE_URL="postgres://postgres:password@127.0.0.1:5432/conference"
cargo sqlx database create
cargo sqlx migrate run
cargo sqlx prepare -- --tests
```

## License

The source code is provided under the terms of [the MIT license][license].

[license]:http://www.opensource.org/licenses/MIT
[travis]:https://travis-ci.com/netology-group/conference?branch=master
[travis-img]:https://travis-ci.com/netology-group/conference.png?branch=master
