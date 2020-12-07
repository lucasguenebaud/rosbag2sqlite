TOPIC_FILTER_MODE = "whitelist" #whitelist/blacklist
PATH = 'test.bag'

from datetime import datetime
import pandas as pd

import rosbag
bag = rosbag.Bag(PATH)

from topic_whitelist import topic_whitelist, topic_blacklist

### Gather the bags manifest ###
infos = bag.get_type_and_topic_info()
manifest = pd.DataFrame(infos.topics).transpose().reset_index()
manifest.columns = ['topic', 'type', 'count','connections','frequency']

### Crop the manifest for the topics we want ###
if TOPIC_FILTER_MODE == "whitelist":
    msgs = manifest.loc[[t in topic_whitelist for t in manifest.topic]].dropna().reset_index(drop=True)
elif TOPIC_FILTER_MODE == "blacklist":
    msgs = manifest.loc[[t not in topic_blacklist for t in manifest.topic]].dropna().reset_index(drop=True)
else:
    raise ValueError("TOPIC_FILTER_MODE usage error. Supported values : 'whitelist' and 'blacklist'")


### Index messages ###
import sqlalchemy
from rospy_message_converter import json_message_converter
import json

BAGID = None
for i,r in msgs.iterrows():
    print(f"Processing {r['topic']}")
    messages = []
    for messagetopic, msg, t in bag.read_messages(topics=r.topic):
        try:
            messageid = int(datetime.now().strftime('%y%m%d%H%M%S%f'))
            messagedata = json_message_converter.convert_ros_message_to_json(msg)
            time_ros = datetime.fromtimestamp(msg.header.stamp.secs + msg.header.stamp.nsecs*1e-9)
            typeid = r['type']
            if BAGID is None:
                BAGID = int(time_ros.strftime('%y%m%d%H%M%S%f'))
                engine = sqlalchemy.create_engine(f'sqlite:///./{BAGID}.sqlite')
                engine.execute("DROP TABLE IF EXISTS bag_message_data;")
            bagid = BAGID
            messages.append((messageid, time_ros, bagid, typeid, messagedata, messagetopic[1:]))
        except:
            print(f"\tWARNING: Message {r['topic']} didn't index")
    df = pd.DataFrame(columns=['messageid', 'time_ros', 'bagid', 'typeid', 'messagedata','messagetopic'], data=messages)
    df.to_sql("bag_message_data", engine, dtype={'messagedata': sqlalchemy.types.JSON}, if_exists='append', index=False)
