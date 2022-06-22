# API

"agent.list" => GET /api/v1/rooms/{id}/agents?{offset}&{limit},

"agent_reader_config.read" => GET /api/v1/rooms/{id}/configs/reader,

"agent_reader_config.update" => POST /api/v1/rooms/{id}/configs/reader,

"agent_writer_config.read" => GET /api/v1/rooms/{id}/configs/writer,

"agent_writer_config.update" => POST /api/v1/rooms/{id}/configs/writer,

"room.close" => POST /api/v1/rooms/{id}/close,

"room.create" => POST /api/v1/rooms,

"room.enter" => POST /api/v1/rooms/{id}/enter,

"room.leave" => POST /api/v1/rooms/{id}/leave,

"room.read" => GET /api/v1/rooms/{id},

"room.update" => PATCH /api/v1/rooms/{id},

"rtc.create" => POST /api/v1/rooms/{id}/rtcs,

"rtc.list" => GET /api/v1/rooms/{id}/rtcs,

"rtc.read" => GET /api/v1/rtcs/{id},

"rtc.connect" => POST /api/v1/rtcs/{id}/streams,

"rtc_signal.create" => POST /api/v1/streams/signal,

"rtc_stream.list" => GET /api/v1/rooms/{id}/streams?{rtc_id}&{time}&{offset}&{limit},

"system.vacuum" => POST /api/v1/system/vacuum,

"writer_config_snapshot.read" => GET /api/v1/rooms/{id}/configs/writer/snapshot
