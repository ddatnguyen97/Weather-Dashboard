from dash import dcc

def create_date_picker(date, initial_date):
    return dcc.DatePickerSingle(
        id='global-date-picker',
        date=date,
        initial_visible_month=initial_date,
        display_format='YYYY-MM-DD',
        first_day_of_week=1,
        className='date-picker',
    )   

def create_hour_picker(initial_hour):
    hours = [f"{h:02d}:00" for h in range(24)]
    return dcc.Dropdown(
        id="global-hour-picker",
        options=[{"label": h, "value": h} for h in hours],
        value=initial_hour,
        clearable=False,
        className="hour-picker",
    )