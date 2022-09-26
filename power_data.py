import os
import numpy as np
import pandas as pd
import pandera as pa
from powerservice import trading
from datetime import datetime, timedelta

import logging 

global previous_time
previous_time = "00:00"
now = datetime.now()
time_tag = now.strftime("%Y%m%d_%H%M")

try:
    os.mkdir('logs')
except:
    pass

def log_error(time_st): 
    log_path = os.path.join('log', f"error_message{time_st}.log")
    logging.basicConfig(filename=log_path, 
                        format='%(asctime)s %(message)s', 
                        filemode='w') 

    logger=logging.getLogger() 
    logger.setLevel(logging.ERROR) 


def hour_within(x):
    global previous_time, previous_minute
    x = x['time']
    if x is np.nan:
        x = datetime.strptime(previous_time, '%H:%M') + timedelta(minutes=5)
    else:
        x = datetime.strptime(x, '%H:%M')
    
    minute_interval = (x - datetime.strptime(previous_time, '%H:%M')).total_seconds()//60
    if minute_interval == -1435.0:
        minute_interval = 5

    previous_time = f"{x.hour}:{x.minute}"
    
    hour = x.hour
    if hour < 10:
        hour = f"0{x.hour}"

    minute = x.minute
    if x.minute < 10:
        minute = f"0{x.minute}"

        
    return [f"{hour}:00", f"{hour}:{minute}", minute_interval ]


class PowerTradersReport():

    def __init__(self, date, output_location=None):
        """
            Input to the class

        Args:
            date (str): must be dd/mm/yyyy format
            output_location (str, optional): folder to save all output file. Defaults to None.
        """
        self.date = date
        self.output_location = output_location


    def get_trade_data(self):
        """
            Fetches trade data and do some data manipulation to extract the
            hour when the data is generated, the time interval and fix 
            any missing time data
        Raises:
            Exception: when the process is not valid, this basically means
            the class input parameter is wrong

        Returns:
            dataframe: pandas dataframe
        """
        try:
            trades = trading.get_trades(self.date)
            df =  pd.DataFrame(trades)
            explode_df = df.apply(pd.Series.explode).reset_index()

            explode_df[['time_fixed', 'hour_within', 'interval']]  = explode_df.apply(hour_within, axis = 1, result_type ='expand')
            explode_df.at[0, 'interval'] = 5

            return explode_df

        except Exception as e:
            logger = log_error(time_tag)
            logger.error("ERROR occured when running get_trade_data")
            logger.error(str(e))

            raise Exception(str(e))
            

    def get_quality_summary(self):
        try:
            schema = pa.DataFrameSchema({
                "date": pa.Column(str, nullable=False),
                "id": pa.Column(str, nullable=False),
                "time_fixed": pa.Column(str, nullable=False, regex=r"(\d{2}:\d{2})"),
                "hour_within": pa.Column(str, regex=r"(\d{2}:00)", nullable=False),
                "interval": pa.Column(float, checks=[pa.Check.equal_to(5)]
                ),
            })

            info = [{
                'time_format': "correct",
                "date_format": "correct",
                'minute_interval': "5min",
                "id": "not null",
            }]
            
            df = self.get_trade_data()
            validated_df = schema(df)

            quality_report = pd.DataFrame(info )
            return quality_report.T.rename_axis('columns').rename(columns={0: "check_result"})


        except Exception as e:
            logger = log_error(time_tag)
            logger.error("ERROR occured when running get_quality_summary")
            logger.error("'error': data schema not correct")
            logger.error(str(e))
            raise Exception(str(e))


    def get_data_profile(self):
        try:
            df = self.get_trade_data()
            profile_df = df.describe(include='object').T.reset_index().rename(columns={'index': 'columns'})
            profile_df['total_missing_value'] = df.shape[0] - profile_df['count']

            return profile_df[['columns', 'count', 'total_missing_value', 'unique']]

        except Exception as e:
            logger = log_error(time_tag)
            logger.error("ERROR occured when running get_data_profile")
            logger.error(str(e))
            raise Exception(str(e))


    def get_data_summary(self):
        try:
            df = self.get_trade_data()
            agg_df = df.groupby(['hour_within'])['volume'].sum().reset_index()

            mapper = {hour: 1+i for i, hour in enumerate(agg_df['hour_within'])}
            mapper['23:00'] = 0

            agg_df['num'] = agg_df['hour_within'].map(mapper)
            final_df = agg_df\
                            .sort_values(by=['num'])\
                            .drop(columns=['num'])\
                            .rename(columns={'hour_within': "Local Time", "volume": "Volume"})

            return final_df

        except Exception as e:
            logger = log_error(time_tag)
            logger.error("ERROR occured when running get_data_summary")
            logger.error(str(e))
            raise Exception(str(e))


    def save_report(self):
        try:
            output_location = self.output_location
            try:
                os.mkdir(output_location)
            except FileExistsError:
                pass
            path = os.path.join(output_location, f'PowerPosition_{time_tag}')
            quality_report = self.get_quality_summary()
            profile_report = self.get_data_profile()
            agg_report = self.get_data_summary()


            quality_report.to_csv(f"{path}_data_profiling.csv", index=False)
            print("-------> Profile report saved as ", f"{path}_data_quality.csv")

            agg_report.to_csv(f"{path}.csv", index=False)
            print("-------> aggregated report saved as ", f"{path}.csv")

            profile_report.to_csv(f"{path}_data_profiling.csv", index=False)
            print("-------> Profile report saved as ", f"{path}_data_profiling.csv")
        
        except Exception as e:
            logger = log_error(time_tag)
            logger.error("ERROR occured when running save_report")
            logger.error(str(e))
            raise Exception(str(e))
        
        