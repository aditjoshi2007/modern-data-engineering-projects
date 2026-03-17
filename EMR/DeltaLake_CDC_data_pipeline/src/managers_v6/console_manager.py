from datetime import datetime
from pytz import timezone

# ------------------------------------------------------------------------------------------------------------
# print operation name in the log
# ------------------------------------------------------------------------------------------------------------
def write_message(msg):
    msg = f"----- {msg} -----"

    if len(msg) > 100:
        dash_line = "-" * 100
    else:
        dash_line = "-" * len(msg)

    date_format = "%d-%m-%Y %I:%M:%S %p"
    date = datetime.now(tz = timezone('US/Central'))
    date = date.astimezone(timezone('US/Central'))
    print(dash_line)
    print("----- Time: " + date.strftime(date_format))
    print(msg)
    print(dash_line)

def write_log(filename, function, level, message):
    cst = timezone('US/Central')
    print(datetime.now(cst).strftime("%d-%m-%Y %I:%M:%S %p") + "::" + filename.split('/')[-1] + "::" + function + "::" + level + "::" + message)

    
    
