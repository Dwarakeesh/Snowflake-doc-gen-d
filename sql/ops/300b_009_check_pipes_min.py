from snowflake.snowpark import Session
import json,os
def check_pipes(session:Session): rows=session.sql("select table_name,pipe_name,stream_name from information_schema.pipes where pipe_name ilike '%PIPE_%'").collect(); res=[r.as_dict() for r in rows]; return{'pipes':res}