insert into "group" (room_id, number)
select id room_id, 0 number
from room
-- TODO: Is this the right way to find future rooms?
where upper(time) > date(now())
and rtc_sharing_policy = 'owned';
