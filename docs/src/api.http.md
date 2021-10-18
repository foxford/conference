# API
/api/v1/
"agent.list" => GET /rooms/{id}/agents?{offset}&{limit},

"agent_reader_config.read" => GET /rooms/{id}/configs/reader,

"agent_reader_config.update" => POST /rooms/{id}/configs/reader,

"agent_writer_config.read" => GET /rooms/{id}/configs/writer,

"agent_writer_config.update" => POST /rooms/{id}/configs/writer,

"room.close" => POST /rooms/{id}/close,

"room.create" => POST /rooms,

"room.enter" => POST /rooms/{id}/enter,

"room.leave" => POST /rooms/{id}/leave,

"room.read" => GET /rooms/{id},

"room.update" => PATCH /rooms/{id},

"rtc.create" => POST /rooms/{id}/rtcs,

"rtc.list" => GET /rooms/{id}/rtcs,

"rtc.read" => GET /rtcs/{id},

"rtc.connect" => POST /rtcs/{id}/streams,

"rtc_signal.create" => POST /streams/signal,

"rtc_stream.list" => GET /rooms/{id}/streams?{rtc_id}&{time}&{offset}&{limit},

"system.vacuum" => POST /system/vacuum,

"writer_config_snapshot.read" => GET /rooms/{id}/configs/writer/snapshot
