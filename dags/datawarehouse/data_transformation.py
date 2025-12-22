from datetime import timedelta, datetime

def parse_duration(duration_str):
    """Parse a duration string in the format 'PT#H#M#S' into a timedelta object."""
    duration_str = duration_str.replace('P','').replace('T','')

    components = ["D", "H", "M", "S"]
    values = {"D":0, "H":0, "M":0, "S":0}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)
    
    total_duration = timedelta(days=values["D"], hours=values["H"], minutes=values["M"], seconds=values["S"])

    return total_duration


def transform_data(row):
    duratration_td = parse_duration(row['Duration'])

    row['Duration'] = (datetime.min + duratration_td).time()
    row['Video_Type'] = "Shorts" if duratration_td.total_seconds() <=60 else "Normal"
    return row