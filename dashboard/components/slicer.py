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
