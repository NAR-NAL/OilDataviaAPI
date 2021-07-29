import simplejson
import pandas as pd
from pandas import DataFrame
from datetime import datetime, date, time
from urllib.error import URLError, HTTPError
from urllib.request import urlopen
import os



def PullMonthlyInventoryandSupplied(read_file,key,sheet_name):
    dict_monthlyinv = {}

    df_monthlyInvkeys = pd.read_excel(read_file,header=0,sheet_name=sheet_name).dropna()
    df_monthlyInvkeys['API_Keys'] = df_monthlyInvkeys['API_Keys'].str.replace('YOUR_API_KEY_HERE', key).astype('str')

    for LFMMvarname, APIkey in zip(df_monthlyInvkeys ['LFMM Variable'].iteritems(), df_monthlyInvkeys ['API_Keys'].iteritems()):
        response = urlopen(APIkey[1])
        raw_byte = response.read()
        raw_string = str(raw_byte, 'utf-8-sig')
        data = simplejson.loads(raw_string)  # what does this do?
        data = data['series'][0]['data']  # pulls the first element from the "series" key and then "data" key
        df = DataFrame.from_records(data)
        df.rename(columns={0: 'Yr', 1: 'US'}, inplace=True)
        df['Var'] = LFMMvarname[1]
        df['Yr'] = pd.to_datetime(df['Yr'], format='%Y%m', errors='coerce').dropna()
        dict_monthlyinv.update({LFMMvarname[1]: df})
    return dict_monthlyinv

def PullWeeklyInventoryandSupplied(read_file, key,sheet_name):
    dict_weeklyinv = {}
    df_weeklyInvkeys = pd.read_excel(read_file, header=0, sheet_name=sheet_name).dropna()
    df_weeklyInvkeys['API_Keys'] = df_weeklyInvkeys['API_Keys'].str.replace('YOUR_API_KEY_HERE', key).astype('str')

    for LFMMvarname, APIkey in zip(df_weeklyInvkeys['LFMM Variable'].iteritems(),df_weeklyInvkeys['API_Keys'].iteritems()):
        response = urlopen(APIkey[1])
        raw_byte = response.read()
        raw_string = str(raw_byte, 'utf-8-sig')
        data = simplejson.loads(raw_string)  # what does this do?
        data = data['series'][0]['data']  # pulls the first element from the "series" key and then "data" key
        df = DataFrame.from_records(data)
        df.rename(columns={0: 'Yr', 1: 'US'}, inplace=True)
        df['Yr'] = pd.to_datetime(df['Yr'], format='%Y/%m/%d', errors='ignore')
        df = df.resample('MS', on='Yr',convention='start').mean()
        df['Var'] = LFMMvarname[1]
        df.reset_index(inplace=True)
        dict_weeklyinv.update({LFMMvarname[1]:df})
    return dict_weeklyinv

def Aggregate_df(dict_monthinv,dict_weekinv):
    appended_df = []
    for monthlyPADDkey,monthlyPADD,weeklyPADD in zip(dict_monthinv.keys(),dict_monthinv.values(),dict_weekinv.values()):
        recent_date = monthlyPADD['Yr'].max()
        fiveyeardate = pd.Timestamp(recent_date.year-5,1,1)
        weeklyPADD = weeklyPADD[weeklyPADD['Yr'] > recent_date]
        df = pd.concat([monthlyPADD, weeklyPADD])
        df = df[df['Yr'] >= fiveyeardate]
        appended_df.append(df)
    appended_df = pd.concat(appended_df)
    appended_df = appended_df.pivot_table(values='US', index='Yr', columns='Var').reset_index()
    appended_df.sort_values(by=['Yr'],inplace=True,ascending=False)
    print(appended_df)
    return appended_df

def CalculateSpreads(df,writer):
    df['NYH-GC (MoGas)'] = df['MoGas NYH'] - df['MoGas USGC']
    df['LA-GC (MoGas)'] = df['MoGas LA'] - df['MoGas USGC']
    df['LA-NYH (MoGas)'] = df['MoGas LA'] - df['MoGas NYH']
    df['NYH-GC (Diesel)'] = df['Diesel NYH'] - df['Diesel USGC']
    df['LA-GC (Diesel)'] = df['Diesel LA'] - df['Diesel USGC']
    df['LA-NYH (Diesel)'] = df['Diesel LA'] - df['Diesel NYH']
    df['USGC CrackSpread'] = (42 * (2 * df['MoGas USGC'] + df['Diesel USGC']) - 3 * df['Brent'])/3
    df['NYH CrackSpread'] =  (42*(2*df['MoGas NYH'] +df['Diesel NYH']) - 3*df['Brent'])/3
    df['LA CrackSpread'] = (42 * (2 * df['MoGas LA'] + df['Diesel LA']) - 3 * df['Brent'])/3
    df['USGC Conv. MoGas Spread'] = 42 * df['MoGas USGC'] - df['Brent']
    df['NYH Conv. MoGas Spread'] = 42 * df['MoGas NYH'] - df['Brent']
    df['LA Conv. MoGas Spread'] = 42 * df['MoGas LA'] - df['Brent']
    df['USGC ULSD Spread'] = 42 * df['Diesel USGC'] - df['Brent']
    df['NYH ULSD Spread'] = 42 * df['Diesel NYH'] - df['Brent']
    df['LA ULSD Spread'] = 42 * df['Diesel LA'] - df['Brent']
    df.to_excel(writer,sheet_name='Prices_spreads')



def InventoryStats(df,writer,monthlysheet):
    df['US'] = df[list(df.columns)].sum(axis=1)
    df_US = df[['Yr','US']]
    #df_US['Max'] = df[list(df_US.US)].max(axis=1)
    #df_US['Min'] = df[list(df_US.US)].min(axis=1)

    #df_US.set_index('Yr',inplace=True)
    #df_US = df_US.resample('AS').mean().reset_index()

    df_US.to_excel(writer, sheet_name=monthlysheet+'_US')
    df.to_excel(writer, sheet_name=monthlysheet)

def CalculateDoS(dict_monthinv,dict_monthps):
    df_DoS = pd.DataFrame()
    for df_monthinv,df_monthps in zip(dict_monthinv.values(),dict_monthps.values()):
        #print(df_monthinv)
        df = df_monthinv.US / df_monthps.US
        df_DoS[df_monthinv.Var.loc[1]+'_DoS'] = df
    df_DoS['Yr'] = df_monthinv.Yr
    recent_date = df_monthinv['Yr'].max()
    fiveyeardate = pd.Timestamp(recent_date.year - 5, 1, 1)
    df_DoS = df_DoS[df_DoS['Yr'] >= fiveyeardate]
    cols = list(df_DoS.columns)
    cols = [cols[-1]] + cols[:-1]
    df_DoS = df_DoS[cols]
    df_DoS.to_excel(writer, sheet_name=df_monthinv.Var.loc[1]+'_DoS')



########################################################################################################################
if __name__ == '__main__':


    directory = os.getcwd()
    filename = './APIkey_text'
    extension = '.xlsx'
    list_monthlysheet = ['Monthly_InvGas','Monthly_InvDiesel','Monthly_prices']
    list_weeklysheet = ['Weekly_InvGas','Weekly_InvDiesel','Weekly_prices']

    list_monthlyinv = ['Monthly_InvGas','Monthly_InvDiesel']
    list_monthlyps = ['Monthly_PSGas', 'Monthly_PSDiesel']

    monthly_productsup = 'Monthly_prodsup'
    writer = pd.ExcelWriter('LFMT_monthlydata.xlsx', engine='xlsxwriter')

    read_file = directory + filename + extension
    key = 'enter your key here'
    for monthlysheet, weeklysheet in zip(list_monthlysheet,list_weeklysheet):
        dict_month = PullMonthlyInventoryandSupplied(read_file, key, monthlysheet)
        dict_week = PullWeeklyInventoryandSupplied(read_file, key, weeklysheet)
        df = Aggregate_df(dict_month, dict_week)
        if (monthlysheet == 'Monthly_prices'):
            CalculateSpreads(df,writer)


    for monthlysheet_inv, monthlysheet_ps in zip(list_monthlyinv,list_monthlyps):
        dict_monthinv = PullMonthlyInventoryandSupplied(read_file, key, monthlysheet_inv)
        dict_monthps = PullMonthlyInventoryandSupplied(read_file, key, monthlysheet_ps)
        CalculateDoS(dict_monthinv,dict_monthps)


writer.save()





