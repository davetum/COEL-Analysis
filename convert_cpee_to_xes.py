# Assumes the index.txt with xes.yaml files from Zenodo in the same directory

import uuid
from typing import List, Any
import pandas as pd

import pm4py
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.objects.log.exporter.xes import exporter as xes_exporter



from yaml import load_all, FullLoader

from datetime import date, datetime

CPEE_INSTANCE_UUID = 'CPEE-INSTANCE-UUID'
#todo: figure out what raw does and if the new key (data) is needed and works flawlessly
CPEE_RAW = 'data'
CPEE_STATE = "CPEE-STATE"


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


OCEL_GLOBAL = "ocel:global-log"
OCEL_VERSION = "ocel:version"
OCEL_VERSION = "ocel:version"
OCEL_ORDERING = "ocel:ordering"
OCEL_ATTN = "ocel:attribute-names"
OCEL_OBJT = "ocel:object-types"
OCEL_GE = "ocel:global-event"
OCEL_GO = "ocel:global-object"
OCEL_ACT = "ocel:activity"
OCEL_TYPE = "ocel:type"
OCEL_EVENTS = "ocel:events"
OCEL_OBJECTS = "ocel:objects"
OCEL_TIME = "ocel:timestamp"
OCEL_OMAP = "ocel:omap"
OCEL_VMAP = "ocel:vmap"
OCEL_OVMAP = "ocel:ovmap"
OCEL_NA = "__INVALID__"
#change again after testing
PATH_PREFIX = './data/'
CONCEPT_INSTANCE = 'concept:instance'
CONCEPT_NAME = 'concept:name'
CONCEPT_ENDPOINT = 'concept:endpoint'
ID = 'id:id'
CPEEID = 'cpee:uuid'
CPEE_ACT_ID = "cpee:activity_uuid"
DATASTREAM = "stream"
DATASTREAM_TO = "stream:to"
ACTIVITY_TO_INSTANCE = "act:instance"
XES_DATASTREAM = "stream:datastream"
XES_DATACONTEXT = "stream:datacontext"
XES_DATASTREAM_NAME = "stream:name"
XES_DATASTREAM_SOURCE = "stream:source"
LIFECYCLE = 'lifecycle:transition'
CPEE_LIFECYCLE = 'cpee:lifecycle:transition'
CPEE_INSTANTIATION = "task/instantiation"
CPEE_RECEIVING = "activity/receiving"
SUB_ROOT = "sub:root"
DATA = 'data'
TIME = 'time:timestamp'
ROOT = 'root'
EVENT = 'event'
EXTERNAL = "external"
DUMMY = ''
EXTERNAL = "external"
NA = "NA"
DICT_TO_LIST = "list"
NAMESPACE_SUBPROCESS = "workflow:sub"
NAMESPACE_LIFECYCLE = "workflow:lc"
NAMESPACE_DEVICES = "workflow:devices"
NAMESPACE_RESOURCES = "workflow:resources"
NAMESPACE_WORKFLOW = "workflow"
CPEE_TIME_STRING = "%Y-%m-%dT%H:%M:%S.%f"
LC_DELIMITER = "§"


class Node:
    def __init__(self, indented_line):
        self.children = []
        self.level = len(indented_line) - len(indented_line.lstrip())
        s = indented_line.strip().split('(')
        self.ot = s[0].strip()
        self.oid = s[1].split(')')[0]

    def add_children(self, nodes, log, data, sub, fail):
        childlevel = nodes[0].level
        while nodes:
            node = nodes.pop(0)
            if node.level == childlevel:  # add node as a child
                self.children.append(node)
                try:
                    temp_trace = read_trace(node.oid)
                    # sub[node.oid] = [temp_trace[0], None]
                    # sub[node.oid][1] = self.ot
                    temp_trace = temp_trace[1:]
                    for event in temp_trace:
                        append_event(self.ot, node.ot, self.oid, node.oid, event, log, ots, data, sub)
                except FileNotFoundError:
                    print(f"Could not read {node.oid}.\n")
                    fail.append(node.oid)
            elif node.level > childlevel:  # add nodes as grandchildren of the last child
                nodes.insert(0, node)
                self.children[-1].add_children(nodes, log_final, data, sub, fail)
            elif node.level <= self.level:  # this node is a sibling, no more children
                nodes.insert(0, node)
                return
            pass

    def as_dict(self):
        if len(self.children) > 1:
            return {self.oid: [node.as_dict() for node in self.children]}
        elif len(self.children) == 1:
            return {self.oid: self.children[0].as_dict()}
        else:
            return self.oid


def read_trace(uuid) -> List[Any]:
    with open(f'{PATH_PREFIX}{uuid}.xes.yaml') as f:
        temp_trace = load_all(f, Loader=FullLoader)
        temp_trace = [i for i in temp_trace]
    return temp_trace


def append_event(ot_parent, ot_child, oid_parent, oid_child, event, log, e_ots, e_data, sub):
    if event[EVENT][CPEE_LIFECYCLE] == "stream/data":
        if XES_DATASTREAM in event[EVENT]:
            data_id = str(uuid.uuid4())
            e_data[DATASTREAM][data_id] = event[EVENT][XES_DATASTREAM]
            log[XES_DATASTREAM].append(data_id)
            if str(event[EVENT][XES_DATASTREAM]).find("context") != -1:
                e_data[DATASTREAM_TO][data_id] = "TODO"
            else:
                # Datastream events without context pertain to the trace
                if oid_child in e_data[DATASTREAM_TO][NAMESPACE_SUBPROCESS]:
                    # Subprocess assigned
                    e_data[DATASTREAM_TO][NAMESPACE_SUBPROCESS][oid_child].append(data_id)
                else:
                    e_data[DATASTREAM_TO][NAMESPACE_SUBPROCESS][oid_child] = [data_id]
                event_actid = event[EVENT][CPEE_ACT_ID]
                if event_actid in e_data[DATASTREAM_TO][CPEE_ACT_ID]:
                    # Task assigned
                    e_data[DATASTREAM_TO][CPEE_ACT_ID][event_actid].append(data_id)
                else:
                    e_data[DATASTREAM_TO][CPEE_ACT_ID][event_actid] = [data_id]
                # e_data[DATASTREAM_TO][data_id] = [oid_child, event[EVENT][CPEE_ACT_ID]]
            # set_nonoptional(event, log, DATASTREAM)
        else:
            log[XES_DATASTREAM].append(DUMMY)
        if XES_DATACONTEXT in event[EVENT]:
            data_id = str(uuid.uuid4())
            e_data[DATASTREAM][data_id] = event[EVENT][XES_DATACONTEXT]
            log[XES_DATACONTEXT].append(data_id)
            # set_nonoptional(event, log, DATACONTEXT)
        else:
            log[XES_DATACONTEXT].append(DUMMY)
    else:
        log[XES_DATASTREAM].append(DUMMY)
        log[XES_DATACONTEXT].append(DUMMY)
    if DATA in event[EVENT]:
        data_id = str(uuid.uuid4())
        e_data[DATA][data_id] = event[EVENT][DATA]
        log[DATA].append(data_id)
    else:
        log[DATA].append(DUMMY)
    if TIME in event[EVENT]:
        set_attribute(event, log, TIME)
    else:
        return
    set_attribute(event, log, CONCEPT_INSTANCE)
    if CONCEPT_NAME in event[EVENT]:
        set_attribute(event, log, CONCEPT_NAME)
    else:
        log[CONCEPT_NAME].append(EXTERNAL)
    if CONCEPT_ENDPOINT in event[EVENT]:
        set_attribute(event, log, CONCEPT_ENDPOINT)
    else:
        log[CONCEPT_ENDPOINT].append(DUMMY)
    if CPEE_ACT_ID in event[EVENT]:
        set_attribute(event, log, CPEE_ACT_ID)
        if log[CONCEPT_NAME][-1] in e_data[ACTIVITY_TO_INSTANCE]:
            if event[EVENT][CPEE_ACT_ID] in e_data[ACTIVITY_TO_INSTANCE][log[CONCEPT_NAME][-1]]:
                e_data[ACTIVITY_TO_INSTANCE][log[CONCEPT_NAME][-1]][event[EVENT][CPEE_ACT_ID]] += 1
            else:
                e_data[ACTIVITY_TO_INSTANCE][log[CONCEPT_NAME][-1]][event[EVENT][CPEE_ACT_ID]] = 1
        else:
            e_data[ACTIVITY_TO_INSTANCE][log[CONCEPT_NAME][-1]] = {event[EVENT][CPEE_ACT_ID]: 1}
    else:
        log[CPEE_ACT_ID].append(EXTERNAL)
        if log[CONCEPT_NAME][-1] in e_data[ACTIVITY_TO_INSTANCE]:
            e_data[ACTIVITY_TO_INSTANCE][log[CONCEPT_NAME][-1]][EXTERNAL] += 1
        else:
            e_data[ACTIVITY_TO_INSTANCE][log[CONCEPT_NAME][-1]] = {EXTERNAL: 1}
    set_attribute(event, log, ID)
    set_attribute(event, log, CPEEID)
    set_attribute(event, log, LIFECYCLE)
    set_attribute(event, log, CPEE_LIFECYCLE)
    # Need to generalize this
    if CPEE_LIFECYCLE in event[EVENT] and event[EVENT][CPEE_LIFECYCLE] == CPEE_INSTANTIATION:
        # Task instantiation logic of CPEE
        oid_instantiate = event[EVENT][CPEE_RAW][CPEE_INSTANCE_UUID]
        log[sub[oid_instantiate]].append(oid_instantiate)
        log[SUB_ROOT].append(ot_child)
        temp_e_ots = e_ots - {sub[oid_instantiate]}
    elif CPEE_LIFECYCLE in event[EVENT] and event[EVENT][CPEE_LIFECYCLE] == CPEE_RECEIVING and CPEE_RAW in event[
        EVENT] and DATA in event[EVENT][CPEE_RAW][0] and CPEE_STATE in event[EVENT][CPEE_RAW][0][DATA]:
        state = event[EVENT][CPEE_RAW][0][DATA].split(CPEE_STATE)[1].split('"')[2]
        if state == "finished":
            oid_instantiated = event[EVENT][CPEE_RAW][0][DATA].split(CPEE_INSTANCE_UUID)[1].split('"')[2]
            # Wait running giving control back to callee logic of CPEE
            # oid_instantiated = subprocess_dict[CPEE_INSTANCE_UUID]
            log[sub[oid_instantiated]].append(oid_instantiated)
            temp_e_ots = e_ots - {sub[oid_instantiated]}
            log[SUB_ROOT].append(ot_child)
            # This is in fact label splitting
            log[CPEE_LIFECYCLE][-1] = "subprocess/receiving"
        else:
            temp_e_ots = e_ots
            log[SUB_ROOT].append(NA)
    # elif CPEE_LIFECYCLE in event[EVENT] and event[EVENT][CPEE_LIFECYCLE] == "activity/receiving" and "concept:endpoint" in event[EVENT] and event[EVENT]["concept:endpoint"] == "https-get://centurio.work/ing/correlators/message/receive/" and "raw" in event[EVENT] and "data" in event[EVENT]["raw"] and "ok" in str(event[EVENT]["raw"]["data"]):
    # Only with domain knowledge possible to set this object id of the signalling subprocess that was forked
    else:
        temp_e_ots = e_ots
        log[SUB_ROOT].append(NA)
    log[ot_child].append(oid_child)
    temp_e_ots = temp_e_ots - set([ot_child, ROOT])
    for t in temp_e_ots:
        log[t].append(DUMMY)


def set_attribute(event, log, key):
    try:
        log[key].append(event[EVENT][key])
    except KeyError:
        log[key].append(NA)


with open(PATH_PREFIX + 'index.txt') as f:
    indented_text = f.read()

    ots = {ot.strip().split('(')[0].strip() for ot in indented_text.splitlines() if ot.strip()}

log_final = {CONCEPT_INSTANCE: [],
             CONCEPT_NAME: [],
             CONCEPT_ENDPOINT: [],
             ID: [],
             CPEEID: [],
             LIFECYCLE: [],
             CPEE_LIFECYCLE: [],
             DATA: [],
             TIME: [],
             ROOT: [],
             XES_DATASTREAM: [],
             XES_DATACONTEXT: [],
             CPEE_ACT_ID: [],
             SUB_ROOT: []}

for ot in ots:
    log_final[ot] = []

data = {DATA: {},
        DATASTREAM: {},
        DATASTREAM_TO: {
            NAMESPACE_SUBPROCESS: {},
            CPEE_ACT_ID: {}
        },
        ACTIVITY_TO_INSTANCE: {}}
subprocesses = {ot.strip().split('(')[1].split(')')[0].strip(): ot.strip().split('(')[0].strip() for ot in
                indented_text.splitlines() if ot.strip()}
fail = []

root = Node(f"{ROOT}({str(uuid.uuid4())})")
root.add_children([Node(line) for line in indented_text.splitlines() if line.strip()], log_final, data, subprocesses,
                  fail)

#my code from here onwards

# important: delete activities of master and starter only at the very end, data is needed for the final log structure
#first add a org:resource key to final log structure#
# some of the activities have multiple org:resources, this indicates that one started the other process
#probably master and starter activities + starter and subrocess activities
#extract info and later use it for the building of the traces in the final_log structure
#if resources are found with master in the key remove them from the log_final structure
#if yes remove those rows from the log_final structure
#if yes, then take the concept:instance key of this row and add a 
log_final['org:resource'] = []
log_final['case:concept:name'] = []
all_resources = [] # maybe not needed 
current_trace = 0 
current_trace_id = None
counter_j = 0
print(len(log_final[CONCEPT_INSTANCE]))
for i in range(len(log_final[CONCEPT_INSTANCE])):
    resource = [] # Default value for potential org resources if no match found
    for ot in ots:
        val = log_final[ot][i]
        if val not in (DUMMY):  
            resource.append(ot)     
    if any("master" in r for r in resource):
        log_final["org:resource"].append(r for r in resource if "master" in r)   
        log_final['case:concept:name'].append(NA) # Append NA if master found since we dont want it in traces later
    elif any("starter" in r for r in resource):
        if len(resource) == 1 and  current_trace_id != log_final[resource[0]][i]:
                current_trace_id = log_final[resource[0]][i]
                current_trace += 1
        log_final["org:resource"].append(r for r in resource if "starter" in r)
        log_final['case:concept:name'].append(NA) # Append NA same as above
    elif len(resource) == 1:
    
        log_final["org:resource"].append(resource[0])   
        log_final['case:concept:name'].append(str(current_trace)) # Append current trace number as string to case concept name
        counter_j += 1
        #print( current_trace , " counter: " , counter_j ) # print current trace and counter j 
    else:
        log_final["org:resource"].append(NA) # Append NA if no resource found
        log_final['case:concept:name'].append(NA) # Append NA if no resource
    
#next extract the send and receive start activities from log_final
#extract the corresponding data_uuid from the log_final structure
#with the data_uuid, extract the messageids/message_names from the data dictionary
#all this in order to get message_id/name for the send and receive activities and include them in the desired .xes log
log_final['message:id'] = []

for i in range(len(log_final[CONCEPT_INSTANCE])): #check all activities in the log_final structure
    message_id = DUMMY  # Default value if no match found
    val = log_final[CONCEPT_ENDPOINT][i]
    if val not in (DUMMY):
        if (("send" in val or "receive" in val) and (log_final[LIFECYCLE][i] == "start")):
            # Extract the data_uuid from the log_final structure
            data_uuid = log_final[DATA][i]
            if data_uuid != DUMMY and data_uuid in data[DATA]:
                # Extract message_id from the data dictionary
                data_row = data[DATA][data_uuid]
                for item in data_row:
                    if isinstance(item, dict) and item.get('name') == 'id':
                        message_id = item.get('value')                        
                    break  # Stop searching once we find a match
    log_final['message:id'].append(message_id)

# Extracting message_id from the log_final structure


    



# Convert log_final to DataFrame
df = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in log_final.items()]))


  # Print the DataFrame to check the content
#print(df["case:concept:name"].to_string())
# Filter out CPEE external events
df = df[df['id:id'] != 'external']
# Filter out NAs in the 'org:resource' column
df = df[df['org:resource'] != NA]
# Filter out rows where 'case:concept:name' is NA
df = df[df['case:concept:name'] != NA]
#eliminate rows with cpee:activity_uuid duplicates
df = df.drop_duplicates(subset=['cpee:activity_uuid'], keep='first')
#remove columns that are not needed for the final log structure such as all of the ots columns, the root, stream:datastream, stream:datacontext, and sub:root
columns_to_drop = [ot for ot in ots] + [ROOT, XES_DATASTREAM, XES_DATACONTEXT, SUB_ROOT, CPEEID, LIFECYCLE, CPEE_LIFECYCLE, DATA]
df.drop(columns=columns_to_drop, inplace=True, errors='ignore')  # Drop specified columns, ignore errors if not found

# Ensure timestamp columns are in datetime format
df = dataframe_utils.convert_timestamp_columns_in_df(df)
df= df.sort_values(by= 'time:timestamp')



# Convert DataFrame to PM4Py event log
event_log = log_converter.apply(df, variant=log_converter.Variants.TO_EVENT_LOG)
   


filtered_data = {k: v for k, v in data['data'].items() if v is not None}
df_data = pd.DataFrame.from_dict(filtered_data, orient='index')
#print(df_data.to_string())
# write Export the DataFrame to a text file
with open(PATH_PREFIX + "data.txt", "w", encoding="utf-8") as f:
    f.write(df_data.to_string())
    

# Export to XES
xes_exporter.apply(event_log, PATH_PREFIX + "output_log.xes")

#print("XES log exported to output_log.xes")


#write the final log structure to a file
with open(PATH_PREFIX + "org_datas_table.txt", "w", encoding="utf-8") as f: 
 f.write(df.to_string())

