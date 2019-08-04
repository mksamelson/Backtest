"""
Simulation New Version.

Description:

This is a large acquisition study where deal which are greater than 20% of acquirer market cap are included since 2010.
All invest/outvest decisions are based on industry group status (advantaged/disadvantaged).
Industry group status changed from deal completion date to date of announcement of 9th full quarter of consolidated
revenues

@author: Matt
"""

import numpy as np
import pandas as pd
import datetime as dt
import os
import sys
import time
import pandas.tseries.offsets as offsets
import xlsxwriter
import urllib
import sqlalchemy


def our_dbaccess_read(sql_string):
    """

    :param sql_string: the content to import
    :return: data from SQL
    """
    cxn_str = 'DRIVER={SQL Server Native Client 11.0};SERVER=192.168.120.15;DATABASE=ModelFactors_Simulation_IC;UID=sa;PWD=0C4secure'
    params = urllib.quote_plus(cxn_str)
    engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params, echo=False)
    conn = engine.connect().connection
    #    cursor = conn.cursor()

    return_df = pd.read_sql(sql_string, conn)

    conn.close()

    return return_df


def trading_calendar(start_date_string='2002-12-31', end_date_string='2018-07-31'):
    """
    Define trading calendar
    :return: holiday_list, tday, datelist
    """

    from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
        USMartinLutherKingJr, USPresidentsDay, GoodFriday, USMemorialDay, \
        USLaborDay, USThanksgivingDay

    # Create US Trading Calendar

    class USTradingCalendar(AbstractHolidayCalendar):
        rules = [
            #            Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
            Holiday('NewYearsDay', month=1, day=1),
            Holiday('Reagan Day', month=6, day=11, year=2004),
            Holiday('New Years 2006', month=1, day=2, year=2006),
            Holiday('New Years 2007', month=1, day=2, year=2007),
            Holiday('New Years 2012', month=1, day=2, year=2012),
            Holiday('New Years 2017', month=1, day=2, year=2017),
            Holiday('Sandy 1', month=10, day=29, year=2012),
            Holiday('Sandy 2', month=10, day=30, year=2012),
            USMartinLutherKingJr,
            USPresidentsDay,
            GoodFriday,
            USMemorialDay,
            Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
            USLaborDay,
            USThanksgivingDay,
            Holiday('Christmas', month=12, day=25, observance=nearest_workday)
        ]

    cal = USTradingCalendar()

    holiday_list = cal.holidays(dt.datetime(2002, 12, 31), dt.datetime(2018, 5, 31))

    tday = offsets.CustomBusinessDay(calendar=cal)

    datelist = (
        pd.DatetimeIndex(start=start_date_string, end=end_date_string, freq=offsets.CustomBusinessDay(calendar=cal)))

    return holiday_list, tday, datelist


def load_data_sql():
    """

    :return:
    """

    print "Start loading data"
    start_time = time.clock()

    acquisitions_df = our_dbaccess_read(r'''Select * From DBO.acquisitions''')
    acquisitions_df['Acquirer Ticker'] = acquisitions_df['Acquirer Ticker'].str.upper()
    acquisitions_df['Target Ticker'] = acquisitions_df['Target Ticker'].str.upper()
    acquisitions_df['Completion Date'] = pd.to_datetime(acquisitions_df['Completion Date'], errors='coerce')
    acquisitions_df['Announcement Date'] = pd.to_datetime(acquisitions_df['Announcement Date'], errors='coerce')


    # Load SPY Weight Pivot Table
    print "Loading daily returns..."
    master_SPY_WEIGHTS_CONSTITUENTS_df_pivot = our_dbaccess_read(r'''Select * From DBO.IndexWeights_Pivot''')
    master_SPY_WEIGHTS_CONSTITUENTS_df_pivot.set_index('Date', inplace=True)
    master_SPY_WEIGHTS_CONSTITUENTS_df_pivot.index = pd.to_datetime(master_SPY_WEIGHTS_CONSTITUENTS_df_pivot.index)
    master_SPY_WEIGHTS_CONSTITUENTS_df_pivot = master_SPY_WEIGHTS_CONSTITUENTS_df_pivot.sort_index(ascending=True)
    master_SPY_WEIGHTS_CONSTITUENTS_df_pivot = master_SPY_WEIGHTS_CONSTITUENTS_df_pivot.loc[
                                               pd.to_datetime('2003-01-01'):]
    print "Daily returns loaded."

    # Load Sales_Rev_Turn
    print "Loading SALES_REV_TURN..."
    SALES_REV_TURN_df = our_dbaccess_read(r'''Select * From DBO.SALES_REV_TURN''')
    SALES_REV_TURN_df = SALES_REV_TURN_df.sort_values(['Ticker','ANNOUNCEMENT_DT'], ascending=[True,False])

    print "SALES_REV_TURN loaded."

    # Load Ticker Daily Return
    print "Loading daily returns..."
    ticker_daily_return_df_pivot = our_dbaccess_read(r'''Select * From DBO.TickerDailyReturn''')
    ticker_daily_return_df_pivot.set_index('Date', inplace=True)
    ticker_daily_return_df_pivot.index = pd.to_datetime(ticker_daily_return_df_pivot.index)
    ticker_daily_return_df_pivot = ticker_daily_return_df_pivot.sort_index(ascending=True)
    ticker_daily_return_df_pivot = ticker_daily_return_df_pivot.loc[pd.to_datetime('2003-01-01'):]
    print "Daily returns loaded."

    # Load Tickers by Custom Industry Group
    print "Loading custom industry group ticker mappings...."
    tickers_custom_industry_groups_df = our_dbaccess_read(r'''Select * From DBO.tickers_by_custom_industry_group''')
    tickers_custom_industry_groups_df['Ticker'] = tickers_custom_industry_groups_df['Ticker'].str.upper()
    tickers_custom_industry_groups_df.set_index('Ticker', inplace=True)
    print "Custom industry group ticker mappings loaded."

    # Load Outvested Industry Group List
    print "Loading list of outvested industry groups ...."
    outvested_GICS_df = our_dbaccess_read(r'''Select * From DBO.outvested_GICS''')
    outvested_GICS_df['Start'] = pd.to_datetime(outvested_GICS_df['Start'])
    outvested_GICS_df['End'] = pd.to_datetime(outvested_GICS_df['End'])
    outvested_GICS_df = outvested_GICS_df.set_index('GIC')
    print "Outvested industry groups list loaded"

    # Load SPXT
    print "Loading SPX returns for scenario period..."
    spxt_returns_df = our_dbaccess_read(r'''Select * From DBO.SPXT_Returns''')
    # spxt_returns_df = pd.read_csv(path + '\\spxt_returns.csv', header=0)
    spxt_returns_df['Date'] = pd.to_datetime(spxt_returns_df['Date'])
    spxt_returns_df = spxt_returns_df.set_index('Date')
    # spxt_returns_df['Return'] = spxt_returns_df['Return'] / 100
    print "SPX returns for scenario period loaded."

    print 'Data loaded, Run time: {} minutes.'.format((time.clock() - start_time) / 60.0)

    return master_SPY_WEIGHTS_CONSTITUENTS_df_pivot, \
           ticker_daily_return_df_pivot, \
           tickers_custom_industry_groups_df, \
           outvested_GICS_df, \
           spxt_returns_df, \
           acquisitions_df, \
           SALES_REV_TURN_df


def load_signals_sql():
    """

    :return:
    """
    print "Loading Quant Signals"
    quant_signal_df_pivot = our_dbaccess_read(r'''Select * From DBO.Quant_Signals''')
    quant_signal_df_pivot['Date'] = pd.to_datetime(quant_signal_df_pivot['Date'])
    quant_signal_df_pivot.set_index('Date', inplace=True)
    quant_signal_df_pivot.sort_index(inplace=True, ascending=True)
    quant_signal_df_pivot = quant_signal_df_pivot.loc[pd.to_datetime('2003-01-01'):]
    print "Quant Signals Loaded"

    print "Loading LRG Signals"
    LRG_second_order_df_pivot = our_dbaccess_read(r'''Select * From DBO.LRG_Signals''')
    LRG_second_order_df_pivot['Date'] = pd.to_datetime(LRG_second_order_df_pivot['Date'])
    LRG_second_order_df_pivot.set_index('Date', inplace=True)
    LRG_second_order_df_pivot.sort_index(inplace=True, ascending=True)
    LRG_second_order_df_pivot = LRG_second_order_df_pivot.loc[pd.to_datetime('2003-01-01'):]
    print "LRG Signals Loaded"

    print "Loading Buybacks Signals"
    buyback_signal_df_pivot = our_dbaccess_read(r'''Select * From DBO.Buyback_Signals''')
    buyback_signal_df_pivot['Date'] = pd.to_datetime(buyback_signal_df_pivot['Date'])
    buyback_signal_df_pivot.set_index('Date', inplace=True)
    buyback_signal_df_pivot.sort_index(inplace=True, ascending=True)
    buyback_signal_df_pivot = buyback_signal_df_pivot.loc[pd.to_datetime('2003-01-01'):]
    print "Buybacks Signals Loaded"

    quant_signal_df_pivot.fillna(np.nan, inplace=True)
    LRG_second_order_df_pivot.fillna(np.nan, inplace=True)
    buyback_signal_df_pivot.fillna(np.nan, inplace=True)

    quant_signal_df_pivot = quant_signal_df_pivot.reindex(columns=sorted(quant_signal_df_pivot.columns))
    LRG_second_order_df_pivot = LRG_second_order_df_pivot.reindex(columns=sorted(LRG_second_order_df_pivot.columns))
    buyback_signal_df_pivot = buyback_signal_df_pivot.reindex(columns=sorted(buyback_signal_df_pivot.columns))

    if (quant_signal_df_pivot.columns != LRG_second_order_df_pivot.columns).sum() + \
            (buyback_signal_df_pivot.columns != LRG_second_order_df_pivot.columns).sum() + \
            (quant_signal_df_pivot.columns != buyback_signal_df_pivot.columns).sum():
        print 'Ticker list of Quant, LRG, and Buybacks are not identical.'
        sys.exit()

    return quant_signal_df_pivot, LRG_second_order_df_pivot, buyback_signal_df_pivot


def data_preprocessing(tickers_custom_industry_groups_df,
                       outvested_GICS_df,
                       master_SPY_WEIGHTS_CONSTITUENTS_df_pivot,
                       tday):
    """
    Creating pivot table for each test. All return tables have the same formate:
    Index: DateTimeIndex, every day
    Columns: Ticker
    value: corresponding values
    :param tickers_custom_industry_groups_df: cell value,
                                            .loc[date, ticker] = 1.0 for an Invested Industry Group
                                            .loc[date, ticker] = 0.0 for an Outvested Industry Group
    :param outvested_GICS_df: demonstrate the Start Date and End Date of Industry Groups that are in the Outvest GICS
    :param master_SPY_WEIGHTS_CONSTITUENTS_df_pivot:
    :param tday: trade day
    :return: ticker_GICS_mapping_pivot
    """

    # Select Tickers that Appear in Both DataFrame
    print 'Create Ticker-GICS Advantaged/Disadvantaged Table'

    # Create ticker GICS mapping pivot table
    ticker_GICS_mapping_pivot = pd.DataFrame(index=master_SPY_WEIGHTS_CONSTITUENTS_df_pivot.index,
                                             columns=master_SPY_WEIGHTS_CONSTITUENTS_df_pivot.columns)
    for idx, ticker in enumerate(ticker_GICS_mapping_pivot.columns, start=1):
        print idx, ticker

        GIC_assignment = tickers_custom_industry_groups_df.loc[ticker, 'GICS_INDUSTRY_GROUP_NAME']

        # Deal with the Date Range
        if GIC_assignment in outvested_GICS_df.index:

            if outvested_GICS_df.loc[GIC_assignment, 'Start'] < ticker_GICS_mapping_pivot.index[0] and \
                    outvested_GICS_df.loc[GIC_assignment, 'End'] > ticker_GICS_mapping_pivot.index[-1]:
                ticker_GICS_mapping_pivot[ticker] = 0.0

            elif outvested_GICS_df.loc[GIC_assignment, 'Start'] < ticker_GICS_mapping_pivot.index[0]:
                ticker_GICS_mapping_pivot.loc[
                ticker_GICS_mapping_pivot.index[0]:outvested_GICS_df.loc[GIC_assignment, 'End'], ticker] = 0.0

            elif outvested_GICS_df.loc[GIC_assignment, 'End'] > ticker_GICS_mapping_pivot.index[-1]:
                ticker_GICS_mapping_pivot.loc[
                (outvested_GICS_df.loc[GIC_assignment, 'Start'] + tday):ticker_GICS_mapping_pivot.index[-1],
                ticker] = 0.0

            else:
                ticker_GICS_mapping_pivot.loc[
                (outvested_GICS_df.loc[GIC_assignment, 'Start'] + tday):outvested_GICS_df.loc[GIC_assignment, 'End'],
                ticker] = 0.0

    # Assume All Other Ticker, Date as Invested Initially
    ticker_GICS_mapping_pivot.fillna(1.0, inplace=True)

    print 'Data Cleaned'

    return ticker_GICS_mapping_pivot


def select_industry(industry_or_sector,
                    industry_or_sector_group_name,
                    quant_signal_df_pivot,
                    LRG_second_order_df_pivot,
                    buyback_signal_df_pivot,
                    ticker_GICS_mapping_pivot,
                    tickers_custom_industry_groups_df):
    """
    Select a group of tickers by specific industry group or by sector name
    Return two sets of tables, one set is selected signal tables, another one is the remaining part
    All table in each set will contain same tickers for columns and same DateIndex for index, so they are in same shape
    :param industry_or_sector: True for selecting by industry group name, False for selecting by sector name
    :param industry_or_sector_group_name: the industry group name or sector group name to be selected
    :param quant_signal_df_pivot: table containing change of growth rate
    :param LRG_second_order_df_pivot: table containing recent revenue growth rate
    :param buyback_signal_df_pivot: table containing buyback proportion
    :param ticker_GICS_mapping_pivot: table containing indicator of whether a ticker at a date is in Invested/Outvested Industry Group
    :return: select_quant_signal_df, select_LRG_second_order_df, select_buyback_signal_df, select_ticker_GICS_mapping_pivot, \
           remain_quant_signal_df, remain_LRG_second_order_df, remain_buyback_signal_df, remain_ticker_GICS_mapping_pivot
    """

    # Select Tickers in Industry Groups or Sectors
    select_ticker_list = []
    if industry_or_sector == 'industry':
        select_ticker_list = tickers_custom_industry_groups_df[
            tickers_custom_industry_groups_df[
                'GICS_INDUSTRY_GROUP_NAME'] == industry_or_sector_group_name].index.tolist()
        select_ticker_list = [ticker for ticker in select_ticker_list if ticker in quant_signal_df_pivot.columns]

    elif industry_or_sector == 'sector':
        select_ticker_list = tickers_custom_industry_groups_df[
            tickers_custom_industry_groups_df['GICS_SECTOR_NAME'] == industry_or_sector_group_name].index.tolist()
        select_ticker_list = [ticker for ticker in select_ticker_list if ticker in quant_signal_df_pivot.columns]

    remain_ticker_list = [ticker for ticker in quant_signal_df_pivot.columns if ticker not in select_ticker_list]

    # Select Tickers Data
    select_quant_signal_df_pivot = quant_signal_df_pivot[select_ticker_list].copy()
    select_LRG_second_order_df_pivot = LRG_second_order_df_pivot[select_ticker_list].copy()
    select_buyback_signal_df_pivot = buyback_signal_df_pivot[select_ticker_list].copy()
    select_ticker_GICS_mapping_pivot = ticker_GICS_mapping_pivot[select_ticker_list].copy()

    remain_quant_signal_df_pivot = quant_signal_df_pivot[remain_ticker_list].copy()
    remain_LRG_second_order_df_pivot = LRG_second_order_df_pivot[remain_ticker_list].copy()
    remain_buyback_signal_df_pivot = buyback_signal_df_pivot[remain_ticker_list].copy()
    remain_ticker_GICS_mapping_pivot = ticker_GICS_mapping_pivot[remain_ticker_list].copy()

    # Sort Columns
    select_quant_signal_df_pivot = select_quant_signal_df_pivot.reindex(
        columns=sorted(select_quant_signal_df_pivot.index))
    select_LRG_second_order_df_pivot = select_LRG_second_order_df_pivot.reindex(
        columns=sorted(select_LRG_second_order_df_pivot.index))
    select_buyback_signal_df_pivot = select_buyback_signal_df_pivot.reindex(
        columns=sorted(select_buyback_signal_df_pivot.index))
    select_ticker_GICS_mapping_pivot = select_ticker_GICS_mapping_pivot.reindex(
        columns=sorted(select_ticker_GICS_mapping_pivot.index))

    remain_quant_signal_df_pivot = remain_quant_signal_df_pivot.reindex(
        columns=sorted(remain_quant_signal_df_pivot.index))
    remain_LRG_second_order_df_pivot = remain_LRG_second_order_df_pivot.reindex(
        columns=sorted(remain_LRG_second_order_df_pivot.index))
    remain_buyback_signal_df_pivot = remain_buyback_signal_df_pivot.reindex(
        columns=sorted(remain_buyback_signal_df_pivot.index))
    remain_ticker_GICS_mapping_pivot = remain_ticker_GICS_mapping_pivot.reindex(
        columns=sorted(remain_ticker_GICS_mapping_pivot.index))

    return select_quant_signal_df_pivot, select_LRG_second_order_df_pivot, select_buyback_signal_df_pivot, select_ticker_GICS_mapping_pivot, \
           remain_quant_signal_df_pivot, remain_LRG_second_order_df_pivot, remain_buyback_signal_df_pivot, remain_ticker_GICS_mapping_pivot


def first_order_decision_table(quant_signal_df_pivot,
                               buyback_signal_df_pivot,
                               path,
                               outvest_threshold,
                               invest_threshold):
    """
    Generate First Order Test Indicator Table by comparing quant_signal_df_pivot, buyback_signal_df_pivot.
    1.0 for pass, 0.0 for fail.
    Logic:
    For quant_signal_df_pivot:
    During the dynamic first order test, no ticker will be Outvested due fail the first order test, so set every value less than outvest_threshold to np.nan because they do not matter
    Prior and Post dynamic first order test, every value greater than invested threshold will be 1.0 (Invested), otherwise 0.0 (Outvested)

    For buyback_signal_df_pivot:
    every value greater than invested threshold will be 1.0 (Invested), otherwise 0.0 (Outvested)

    If quant_signal_df_pivot value is -99999, we maintain the previous decision, so -99999 will be replaced by np.nan,
    and np.nan will be dealt with by ffill()

    :param quant_signal_df_pivot: table containing change of growth rate
    :param buyback_signal_df_pivot: table containing buyback proportion
    :param path: output path of first_order_indicator_df
    :param outvest_threshold: the threshold whether a ticker fail the first order test, default -0.20
    :param invest_threshold: the threshold whether a ticker pass the first order test, default 0.10
    :return:first_order_indicator_df: indicate whether a signal pass the first order test, only contain 1.0 for pass, 0.0 for fail
    """

    print 'Creating First Order Indicator Table'
    start_time = time.clock()

    # Dynamic First Order Test Date Range
    print 'Dynamic First Order Test'
    adj_start_date = pd.to_datetime('2009-02-01')
    adj_end_date = pd.to_datetime('2009-02-01') + 189 * tday

    # Dynamic First Order Test Mask
    quant_signal_df_pivot_before_dynamic = quant_signal_df_pivot.loc[:adj_start_date - 1 * tday].copy()
    quant_signal_df_pivot_during_dynamic = quant_signal_df_pivot.loc[adj_start_date:adj_end_date].copy()
    quant_signal_df_pivot_after_dynamic = quant_signal_df_pivot.loc[adj_end_date + 1 * tday:].copy()

    quant_signal_df_pivot_before_dynamic[quant_signal_df_pivot_before_dynamic > invest_threshold] = 1.0
    quant_signal_df_pivot_before_dynamic[(quant_signal_df_pivot_before_dynamic < outvest_threshold) &
                                         (
                                                     quant_signal_df_pivot_before_dynamic != -99999)] = 0.0  # Keep -99999 insufficient data flag

    quant_signal_df_pivot_during_dynamic[quant_signal_df_pivot_during_dynamic > invest_threshold] = 1.0
    quant_signal_df_pivot_during_dynamic[(quant_signal_df_pivot_during_dynamic < outvest_threshold) &
                                         (
                                                     quant_signal_df_pivot_during_dynamic != -99999)] = np.nan  # Keep -99999 insufficient data flag

    quant_signal_df_pivot_after_dynamic[quant_signal_df_pivot_after_dynamic > invest_threshold] = 1.0
    quant_signal_df_pivot_after_dynamic[(quant_signal_df_pivot_after_dynamic < outvest_threshold) &
                                        (
                                                    quant_signal_df_pivot_after_dynamic != -99999)] = 0.0  # Keep -99999 insufficient data flag

    tmp = quant_signal_df_pivot_before_dynamic.append(quant_signal_df_pivot_during_dynamic)
    tmp = tmp.append(quant_signal_df_pivot_after_dynamic)  # Append

    quant_signal_df_pivot = tmp
    quant_signal_df_pivot = quant_signal_df_pivot.reindex(columns=sorted(quant_signal_df_pivot.columns))
    quant_signal_df_pivot.iloc[0] = ticker_GICS_mapping_pivot.iloc[
        0]  # Set the beginning indicator to be original industry group assignment on the top

    # Set All -99999 Insufficient Data to Original Industry Groups Assignment
    # print 'Set Error Data.'
    # i = 1
    # for col in quant_signal_df_pivot.columns:
    #     for date in quant_signal_df_pivot.index:
    #         if quant_signal_df_pivot.loc[date, col] == -99999:
    #             print i, date, col
    #             i += 1
    #             quant_signal_df_pivot.loc[date, col] = ticker_GICS_mapping_pivot.loc[date, col]

    # replace all noOutvestSignal and noInvestSignal to np.nan
    # Because when the signal is not actionable, default the previous value
    quant_signal_df_pivot[(outvest_threshold <= quant_signal_df_pivot) &
                          (quant_signal_df_pivot <= invest_threshold) &
                          (quant_signal_df_pivot != 0)] = np.nan

    # Set All -99999 Insufficient Data to Original Industry Groups Assignment
    quant_signal_df_pivot[quant_signal_df_pivot == -99999] = ticker_GICS_mapping_pivot[quant_signal_df_pivot == -99999]

    # Buybacks Signal Masks
    print 'Calculate Buyback Signal.'
    buyback_signal_df_pivot[buyback_signal_df_pivot > 0.05] = 1.0
    buyback_signal_df_pivot[buyback_signal_df_pivot <= 0.05] = np.nan
    buyback_signal_df_pivot = buyback_signal_df_pivot.reindex(columns=sorted(buyback_signal_df_pivot.columns))

    # Pivot table
    print 'Add Revenue Signal and Buyback Signal.'
    first_order_indicator_df = quant_signal_df_pivot.add(buyback_signal_df_pivot, fill_value=0)
    first_order_indicator_df = first_order_indicator_df.replace(2.0, 1.0)

    # Forward Fill
    first_order_indicator_df = first_order_indicator_df.ffill()

    print 'First Order Indicatot Table Created.'
    print 'Run time: {0:.5f} min'.format((time.clock() - start_time) / 60.0)

    # Output the file
    first_order_indicator_df.to_csv(path + '\\first_order_indicator_shift.csv')

    return first_order_indicator_df


def second_order_decision_table(first_order_indicator_df,
                                LRG_second_order_df_pivot,
                                ticker_GICS_mapping_pivot,
                                path,
                                dynamic_second_order_indicator,
                                secondOrderQuantScreen):
    """
    Generate Second Order Test Indicator Table by comparing first_order_indicator_df, LRG_second_order_df
    1.0 for pass, 0.0 for fail

    Logic:
    For first_order_indicator: 1.0 for pass the first order test, vice versa

    For LRG_second_order_df:
    SecondOrderQuantScreen is 0.02 (default value) when no dynamic second order test is applied
    SecondOrderQuantScreen is the current CPI 12 month moving average
    Calculate new CPI 12 month moving average on [1,4,7,10] month and keep that value for a quarter
    Caution: note 2017/4/14 in CPI_df is a good Friday. Actual SecondOrderQuantScreen Change happens on 2017/4/17

    Every value greater than SecondOrderQuantScreen will be 1.0 (Invested), otherwise 0.0 (Outvested)

    If LRG_second_order_df value is -99999, we default to first_order_indicator_df, so -99999 will be replaced by np.nan
    If the sum of second_order_indicator_df value and ticker_GICS_mapping_pivot value is np.nan,
    we default to first_order_indicator_value, which essentially means that always pass the second order test in this case

    All np.nan will be replace with by 1.0, because when there is an np.nan, we should default to first_order_indicator_df.
    second order test automatically passes

    A ticker can only be Invested when it passes both first order test and second order test.
    Note that for a ticker in Advantage Industry Group, ticker_GICS_mapping_pivot value will always be 1.0.
    So the sum of ticker_GICS_mapping_pivot and second_order_indicator will always be either 1.0 or 2.0, so they always pass the second order test

    :param first_order_indicator_df:
    :param LRG_second_order_df: latestRevGrowth table
    :param ticker_GICS_mapping_pivot: table containing indicator of whether a ticker at a date is in Invested/Outvested Industry Group
    :param path: output path of second_order_table
    :param dynamic_second_order_indicator: Indicator of whether to use CPI 12 month moving average as a second order test threshold
    :param secondOrderQuantScreen: Duck type second order threshold, could be a df (in dynamic) or a float (in static)
    :return:ticker_signal_status_df, cpi_announcement_date_check_df
    """

    print "Creating Second Order Decision Table (Determining Decision based on Second Order Test)."
    start_time = time.clock()

    # Dynamic Second Order Test using CPI 12 month Moving Average
    if dynamic_second_order_indicator == True:

        print "Dynamic Second Order Test."

        # Get CPI 12 month moving average
        # TODO, change path
        CPI_df = our_dbaccess_read(r'''SELECT * FROM DBO.CPI''')
        CPI_df['CPI_announcement_date'] = pd.to_datetime(CPI_df['CPI_announcement_date'])
        CPI_df.set_index('CPI_announcement_date', inplace=True)
        CPI_df['CPI_12_month_rolling_average'] = CPI_df['CPI_12_month_rolling_average'] / 100.0

        # Reshape to match other tables, so that we can use a pandas df elemental wise value comparison
        CPI_df = CPI_df.resample('D').ffill()  # Resample to calendar date and forward fill the value
        CPI_df.index = CPI_df.index.date
        CPI_df = CPI_df.reindex(
            LRG_second_order_df_pivot.index)  # Keep only the calendar date that has overlap with LRG
        CPI_df = CPI_df.ffill()
        CPI_df = CPI_df[['CPI_12_month_rolling_average'] * len(
            first_order_indicator_df.columns)]  # Duplicate the columns so that CPI_df has the same shape with all other tables
        CPI_df.columns = LRG_second_order_df_pivot.columns  # Change columns names to match ticker

        CPI_df.to_csv(path + '\\cpi_check_df.csv')
        secondOrderQuantScreen = CPI_df

    # Static Second Order Test
    else:
        print "Static Second Order Test."

    # Create Second Order Table
    second_order_indicator_df = LRG_second_order_df_pivot.copy()

    second_order_indicator_df[second_order_indicator_df > secondOrderQuantScreen] = 1.0
    second_order_indicator_df[
        (second_order_indicator_df <= secondOrderQuantScreen) & (second_order_indicator_df != -99999)] = 0.0
    second_order_indicator_df[second_order_indicator_df == -99999] = np.nan
    second_order_indicator_df.fillna(1.0, inplace=True)

    ticker_signal_status_df = ticker_GICS_mapping_pivot.add(second_order_indicator_df,
                                                            fill_value=0.0)  # Pass either one, pass the second order test
    ticker_signal_status_df = ticker_signal_status_df.replace(2.0,
                                                              1.0)  # When both of the value are 1.0, will generate a 2.0. Replcae them

    ticker_signal_status_df = ticker_signal_status_df.multiply(
        first_order_indicator_df)  # Can only be Invested when a name both pass the first order test and second order test

    # Output the file
    ticker_signal_status_df.to_csv(path + '\\ticker_signal_status.csv')

    print '\nRun Time: ', float(time.clock() - start_time) / 60.0, "minutes"
    print "Second Order Decision Table Created"

    return ticker_signal_status_df


def investment_status_table_create(ticker_signal_status_df,
                                   path):
    """
    Create investment_status_df.
    1.0 for ticker in the portfolio and need to compute weighted return that date. 0.0 otherwise.

    If company releases revenue, buyback on day one, we will calculate the signal and buy/sell (if it is an actionable buy/sell signal)
    on day two, so that the ticker will be in/out the portoflio on day three.

    Therefore, the investment status will always lag for two days compared with ticker _signal_status_df

    :param ticker_signal_status_df:
    :return: ticker_investment_status_df
    """
    print 'Creating Investement Status Dataframe'

    ticker_investment_status_df = ticker_signal_status_df.copy()
    ticker_investment_status_df.index = ticker_investment_status_df.index + offsets.Day(
        2)  # Shift the index up by two days
    ticker_investment_status_df = (ticker_signal_status_df.iloc[0:2]).append(
        ticker_investment_status_df.iloc[:-2])  # Fill the first two days with default ticker_signal_status_df value

    # Output the file
    ticker_investment_status_df.to_csv(path + '\\ticker_investment_status.csv')

    print 'Investement Status Dataframe Created'

    return ticker_investment_status_df


def portfolio_returns_calculation(ticker_investment_status_df,
                                  ticker_daily_return_df_pivot,
                                  master_SPY_WEIGHTS_CONSTITUENTS_df_pivot,
                                  path):
    """
    Calculate the portfolio daily return, daily invested weight

    :param datelist:
    :param ticker_investment_status_df:
    :param adj_close_prices_df:
    :param master_SPY_WEIGHTS_CONSTITUENTS_df_pivot:
    :return: ticker_portfolio_returns_df
    """

    print "Calculating Daily Portfolio Returns"
    start_time = time.clock()

    # Compute ticker daily return
    # TODO replace with returns from Bloomberg
    # adj_close_prices_df['daily return'] = (adj_close_prices_df['PX_LAST'] / adj_close_prices_df['PX_LAST'].shift(1)) - 1.0
    # adj_close_prices_df = adj_close_prices_df[adj_close_prices_df['Date'] >= pd.to_datetime('2002-12-31')]  # Delete redundant value
    #
    # ticker_daily_return_df_pivot = adj_close_prices_df.pivot_table(index='Date', columns='Ticker',
    #                                                                values='daily return')
    # ticker_daily_return_df_pivot.fillna(0.0, inplace=True)  # Deal with np.nan
    #
    # # TODO: we may delete this for we only pull data using SPY weights
    # ticker_daily_return_df_pivot = ticker_daily_return_df_pivot[
    #     [name for name in ticker_daily_return_df_pivot.columns if name in ticker_investment_status_df.columns]] # Delete redundant tickers

    # Sort for np.dot vectorized multiplication
    ticker_daily_return_df_pivot = ticker_daily_return_df_pivot.reindex(
        columns=sorted(ticker_daily_return_df_pivot.columns))
    ticker_investment_status_df_pivot = ticker_investment_status_df.reindex(
        columns=sorted(ticker_investment_status_df.columns))

    # Store portfolio returns
    ticker_portfolio_returns_df = pd.DataFrame(index=ticker_daily_return_df_pivot.index,
                                               columns=['portfolio_daily_return',
                                                        'IV daily return',
                                                        'Daily IV Weight'])

    # Calculate portfolio daily return
    for date in ticker_investment_status_df_pivot.index:
        if date in ticker_daily_return_df_pivot.index:
            print 'Calculating', date

            ticker_portfolio_returns_df.loc[date, 'Daily IV Weight'] = np.dot(
                master_SPY_WEIGHTS_CONSTITUENTS_df_pivot.loc[date],
                ticker_investment_status_df.loc[date])

            ticker_portfolio_returns_df.loc[date, 'IV daily return'] = np.dot(
                np.multiply(master_SPY_WEIGHTS_CONSTITUENTS_df_pivot.loc[date],
                            ticker_investment_status_df.loc[date]),
                ticker_daily_return_df_pivot.loc[date])

    # Gross up daily return by portfolio invested weights of SPX
    ticker_portfolio_returns_df['portfolio_daily_return'] = \
        ticker_portfolio_returns_df['IV daily return'] / ticker_portfolio_returns_df['Daily IV Weight']

    # Output the file
    ticker_portfolio_returns_df.to_csv(path + '\\ticker_portfolio.returns_shift.csv')

    print '\nRun Time: ', float(time.clock() - start_time) / 60.0, "minutes"
    print "Portfolio Daily Returns Calculated"

    return ticker_portfolio_returns_df


def transaction_table(master_SPY_WEIGHTS_CONSTITUENTS_df_pivot, ticker_signal_status_df, ticker_GICS_mapping_pivot,
                      quant_signal_df_pivot, LRG_second_order_df_pivot, buyback_signal_df_pivot, path):
    """
    Capture every trade and evry signal change in a backtest
    :param master_SPY_WEIGHTS_CONSTITUENTS_df_pivot:
    :param ticker_signal_status_df:
    :param ticker_GICS_mapping_df:
    :param quant_signal_df_pivot:
    :param LRG_second_order_df_pivot:
    :param buyback_signal_df_pivot:
    :return:
    """

    print "Create Transaction Reference Table"
    start_time = time.clock()

    print 'Generating Trade Table'
    trade_df = ticker_signal_status_df.multiply((master_SPY_WEIGHTS_CONSTITUENTS_df_pivot != 0.0).astype('float'))

    trade_df = (trade_df != trade_df.shift(1)).astype('float')
    trade_df.iloc[0] = 0.0
    trade_df.reset_index(inplace=True)
    trade_df = trade_df.melt(id_vars='index', value_vars=trade_df.columns[1:], var_name='Ticker', value_name='Trade')
    trade_df = trade_df[trade_df['Trade'] == 1.0]
    trade_df = trade_df.sort_values(['index', 'Ticker'])
    trade_df.rename(columns={'index': 'Date'}, inplace=True)
    trade_df.index = range(len(trade_df))

    print 'Melting Down Pivot Table'
    quant_melt = (quant_signal_df_pivot.reset_index()).melt(id_vars='Date',
                                                            value_vars=quant_signal_df_pivot.columns[1:],
                                                            var_name='Ticker', value_name='First_Order_Signal')
    buyback_melt = (buyback_signal_df_pivot.reset_index()).melt(id_vars='Date',
                                                                value_vars=buyback_signal_df_pivot.columns[1:],
                                                                var_name='Ticker', value_name='Buyback_Signal')
    LRG_melt = (LRG_second_order_df_pivot.reset_index()).melt(id_vars='Date',
                                                              value_vars=LRG_second_order_df_pivot.columns[1:],
                                                              var_name='Ticker', value_name='Second_Order_Signal')
    mapping_melt = (ticker_GICS_mapping_pivot.reset_index()).melt(id_vars='index',
                                                                  value_vars=ticker_GICS_mapping_pivot.columns[1:],
                                                                  var_name='Ticker', value_name='Industry_Status')
    signal_melt = (ticker_signal_status_df.reset_index()).melt(id_vars='index',
                                                               value_vars=ticker_signal_status_df.columns[1:],
                                                               var_name='Ticker', value_name='Trade_Signal')
    mapping_melt.rename(columns={'index': 'Date'}, inplace=True)
    signal_melt.rename(columns={'index': 'Date'}, inplace=True)

    print 'Merging with Trade Table'
    trade_df = trade_df.merge(quant_melt, how='left', left_on=['Date', 'Ticker'], right_on=['Date', 'Ticker'])
    trade_df = trade_df.merge(buyback_melt, how='left', left_on=['Date', 'Ticker'], right_on=['Date', 'Ticker'])
    trade_df = trade_df.merge(LRG_melt, how='left', left_on=['Date', 'Ticker'], right_on=['Date', 'Ticker'])
    trade_df = trade_df.merge(mapping_melt, how='left', left_on=['Date', 'Ticker'], right_on=['Date', 'Ticker'])
    trade_df = trade_df.merge(signal_melt, how='left', left_on=['Date', 'Ticker'], right_on=['Date', 'Ticker'])

    trade_df = trade_df.reindex(
        columns=['Date', 'Ticker', 'Industry_Status', 'Trade_Signal', 'First_Order_Signal', 'Second_Order_Signal',
                 'Buyback_Signal'])

    print 'Generate First Order Signal Change Table'
    quant_signal_change_df = quant_signal_df_pivot.multiply(
        (master_SPY_WEIGHTS_CONSTITUENTS_df_pivot != 0.0).astype('float'))
    quant_signal_change_df.reset_index(inplace=True)
    quant_signal_change_df = quant_signal_change_df.melt(id_vars='Date', value_vars=quant_signal_change_df.columns[1:],
                                                         var_name='Ticker', value_name='Trade')
    quant_signal_change_df = quant_signal_change_df[pd.notnull(quant_signal_change_df['Trade'])]
    quant_signal_change_df = quant_signal_change_df.sort_values(['Date', 'Ticker'])
    quant_signal_change_df.index = range(len(quant_signal_change_df))

    quant_signal_change_df = quant_signal_change_df.merge(quant_melt, how='left', left_on=['Date', 'Ticker'],
                                                          right_on=['Date', 'Ticker'])
    quant_signal_change_df = quant_signal_change_df.merge(buyback_melt, how='left', left_on=['Date', 'Ticker'],
                                                          right_on=['Date', 'Ticker'])
    quant_signal_change_df = quant_signal_change_df.merge(LRG_melt, how='left', left_on=['Date', 'Ticker'],
                                                          right_on=['Date', 'Ticker'])
    quant_signal_change_df = quant_signal_change_df.merge(mapping_melt, how='left', left_on=['Date', 'Ticker'],
                                                          right_on=['Date', 'Ticker'])
    quant_signal_change_df = quant_signal_change_df.reindex(
        columns=['Date', 'Ticker', 'Industry_Status', 'Trade_Signal', 'First_Order_Signal', 'Second_Order_Signal',
                 'Buyback_Signal'])

    print 'Generate Second Order Signal Change Table'
    LRG_signal_change_df = (LRG_second_order_df_pivot.fillna(0.0)).multiply(
        (master_SPY_WEIGHTS_CONSTITUENTS_df_pivot != 0.0).astype('float'))
    LRG_signal_change_df = (LRG_signal_change_df != LRG_signal_change_df.shift(1)).astype('float')
    LRG_signal_change_df.iloc[0] = 0.0
    LRG_signal_change_df.reset_index(inplace=True)
    LRG_signal_change_df = LRG_signal_change_df.melt(id_vars='Date', value_vars=LRG_signal_change_df.columns[1:],
                                                     var_name='Ticker', value_name='Trade')
    LRG_signal_change_df = LRG_signal_change_df[LRG_signal_change_df['Trade'] == 1.0]
    LRG_signal_change_df = LRG_signal_change_df.sort_values(['Date', 'Ticker'])
    LRG_signal_change_df.index = range(len(LRG_signal_change_df))
    LRG_signal_change_df = LRG_signal_change_df.merge(quant_melt, how='left', left_on=['Date', 'Ticker'],
                                                      right_on=['Date', 'Ticker'])
    LRG_signal_change_df = LRG_signal_change_df.merge(buyback_melt, how='left', left_on=['Date', 'Ticker'],
                                                      right_on=['Date', 'Ticker'])
    LRG_signal_change_df = LRG_signal_change_df.merge(LRG_melt, how='left', left_on=['Date', 'Ticker'],
                                                      right_on=['Date', 'Ticker'])
    LRG_signal_change_df = LRG_signal_change_df.merge(mapping_melt, how='left', left_on=['Date', 'Ticker'],
                                                      right_on=['Date', 'Ticker'])
    LRG_signal_change_df = LRG_signal_change_df.reindex(
        columns=['Date', 'Ticker', 'Industry_Status', 'Trade_Signal', 'First_Order_Signal', 'Second_Order_Signal',
                 'Buyback_Signal'])

    print 'Generate Buyback Signal Change Table'
    buyback_signal_change_df = (buyback_signal_df_pivot.fillna(0.0)).multiply(
        (master_SPY_WEIGHTS_CONSTITUENTS_df_pivot != 0.0).astype('float'))
    buyback_signal_change_df = (buyback_signal_change_df != buyback_signal_change_df.shift(1)).astype('float')
    buyback_signal_change_df.iloc[0] = 0.0
    buyback_signal_change_df.reset_index(inplace=True)
    buyback_signal_change_df = buyback_signal_change_df.melt(id_vars='Date',
                                                             value_vars=buyback_signal_change_df.columns[1:],
                                                             var_name='Ticker', value_name='Trade')
    buyback_signal_change_df = buyback_signal_change_df[buyback_signal_change_df['Trade'] == 1.0]
    buyback_signal_change_df = buyback_signal_change_df.sort_values(['Date', 'Ticker'])
    buyback_signal_change_df.index = range(len(buyback_signal_change_df))
    buyback_signal_change_df = buyback_signal_change_df.merge(quant_melt, how='left', left_on=['Date', 'Ticker'],
                                                              right_on=['Date', 'Ticker'])
    buyback_signal_change_df = buyback_signal_change_df.merge(buyback_melt, how='left', left_on=['Date', 'Ticker'],
                                                              right_on=['Date', 'Ticker'])
    buyback_signal_change_df = buyback_signal_change_df.merge(LRG_melt, how='left', left_on=['Date', 'Ticker'],
                                                              right_on=['Date', 'Ticker'])
    buyback_signal_change_df = buyback_signal_change_df.merge(mapping_melt, how='left', left_on=['Date', 'Ticker'],
                                                              right_on=['Date', 'Ticker'])
    buyback_signal_change_df = buyback_signal_change_df.reindex(
        columns=['Date', 'Ticker', 'Industry_Status', 'Trade_Signal', 'First_Order_Signal', 'Second_Order_Signal',
                 'Buyback_Signal'])

    tmp = trade_df.append(quant_signal_change_df)
    tmp = tmp.append(buyback_signal_change_df)
    tmp = tmp.append(LRG_signal_change_df)
    tmp = tmp.drop_duplicates(subset=['Date', 'Ticker'], keep='first')
    tmp = tmp.sort_values(['Date', 'Ticker'])
    tmp.index = range(len(tmp))
    tmp = tmp.set_index('Date')

    print '\nRun Time: ', float(time.clock() - start_time) / 60.0, "minutes"
    print "Transaction Reference Table Created"

    tmp.to_csv(path + '\\comparison_df.csv')

    return trade_df


def annual_and_cumulative_returns(ticker_portfolio_returns_df,
                                  spxt_returns_df,
                                  path, filename):
    """
    Calculate the annual and cumulative returns
    Write them to a excel spread sheet, together with SPX annual and cumulative returns

    :param ticker_portfolio_returns_df:
    :param spxt_returns_df:
    :param path: output file path
    :param filename: the file name
    :return: None
    """

    print "Calculating Annual and Cumulative Portfolio Returns"
    start_time = time.clock()

    # Strategy Annual Returns
    ticker_portfolio_returns_df.loc[pd.to_datetime('2002-12-31')] = 0
    ticker_portfolio_returns_df = ticker_portfolio_returns_df.sort_index()

    strategy_returns = ticker_portfolio_returns_df.loc[:, 'portfolio_daily_return']
    strategy_ret_index = (1 + strategy_returns).cumprod()

    strategy_annual_returns = strategy_ret_index.resample('A-DEC', how='last').pct_change()

    strategy_annual_returns = strategy_annual_returns * 100.0

    print "\n\nStrategy Annual Returns\n\n"
    print strategy_annual_returns

    # Strategy cumulative Return
    strategy_cumulative_returns = strategy_ret_index.sort_index().groupby([strategy_ret_index.index.year]).last()

    print "\n\nStrategy Cumulative Returns\n\n"
    print strategy_cumulative_returns

    # First Half (1/1/2003 - 12/31/2009)
    ticker_portfolio_returns_first_half_df = ticker_portfolio_returns_df.loc[:, 'portfolio_daily_return']
    strategy_returns_first_half = ticker_portfolio_returns_first_half_df[ticker_portfolio_returns_first_half_df.index
                                                                         <= pd.to_datetime('2009-12-31')].copy()
    strategy_ret_first_half_index = (1 + strategy_returns_first_half).cumprod()

    print "\n\nStrategy Cumulative Returns - First Half\n\n"
    print strategy_ret_first_half_index.sort_index().groupby([strategy_ret_first_half_index.index.year]).last()

    # second half
    ticker_portfolio_returns_second_half_df = ticker_portfolio_returns_df.loc[:, 'portfolio_daily_return']
    strategy_returns_second_half = ticker_portfolio_returns_second_half_df[ticker_portfolio_returns_second_half_df.index
                                                                           >= pd.to_datetime('2009-12-31')].copy()
    strategy_returns_second_half.loc[pd.to_datetime('2009-12-31')] = 0
    strategy_ret_second_half_index = (1 + strategy_returns_second_half).cumprod()

    print "\n\nStrategy Cumulative Returns - Second Half\n\n"
    print strategy_ret_second_half_index.sort_index().groupby([strategy_ret_second_half_index.index.year]).last()

    # Monthly Strategy Returns
    strategy_monthly_returns = strategy_ret_index.resample('M', how='last').pct_change()

    print "\n\nStrategy Monthly Returns\n\n"
    print strategy_monthly_returns

    # Monthly cumulative Return
    monthly_cumulative_returns = strategy_ret_index.sort_index().groupby([strategy_ret_index.index.year,
                                                                          strategy_ret_index.index.month]).last()
    print "\n\nMonthly Cumulative Returns\n\n"
    print monthly_cumulative_returns

    # Quarterly cumulative Return
    quarterly_cumulative_returns = strategy_ret_index.sort_index().groupby([strategy_ret_index.index.year,
                                                                          strategy_ret_index.index.quarter]).last()
    print "\n\nMonthly Cumulative Returns\n\n"
    print quarterly_cumulative_returns

    quarterly_cumulative_returns.to_csv('option_4_quarterly_returns.csv')


    # SPXT Returns
    spxt_returns_df.loc[pd.to_datetime('2002-12-31')] = 0
    spx_index_returns_df = spxt_returns_df.sort_index()

    spx_index_returns = spx_index_returns_df.loc[:, 'Return']
    spx_index_ret_index = (1 + spx_index_returns).cumprod()

    spxt_annual_returns = spx_index_ret_index.resample('A-DEC', how='last').pct_change()

    spxt_annual_returns = spxt_annual_returns * 100.0

    print "\n\nSPXT Annual Returns\n\n"
    print spxt_annual_returns

    # SPXT cumulative Return
    spxt_cumulative_returns = spx_index_ret_index.sort_index().groupby([spx_index_ret_index.index.year]).last()

    print "\n\nSPXT Cumulative Returns\n\n"
    print spxt_cumulative_returns

    print '\nRun Time: ', float(time.clock() - start_time) / 60.0, "minutes"
    print "Annual and Cumulative Portfolio Returns Created"

    # SPXT First Half (1/1/2003 - 12/31/2009)
    spx_index_first_half_returns_df = spx_index_returns_df[spx_index_returns_df.index
                                                           <= pd.to_datetime('2009-12-31')].copy()
    spxt_first_half_index = (1 + spx_index_first_half_returns_df['Return']).cumprod()

    # SPXT Second Half
    spx_index_second_half_returns_df = spx_index_returns_df[spx_index_returns_df.index
                                                            >= pd.to_datetime('2009-12-31')].copy()
    spx_index_second_half_returns_df.loc[pd.to_datetime('2009-12-31')] = 0
    spxt_second_half_index = (1 + spx_index_second_half_returns_df['Return']).cumprod()

    # write the cumulative result to a excel spread sheet
    os.chdir(path)
    workbook = xlsxwriter.Workbook(filename, {'nan_inf_to_errors': True, 'in_memory': True})

    # Create formats
    bold_center = workbook.add_format({'bold': True, 'text_wrap': True, 'align': 'center'})
    bold_center.set_bottom()
    merge_format = workbook.add_format({'bold': True, 'align': 'center'})
    merge_format.set_bottom()
    number = workbook.add_format({'num_format': '#,##0'})
    number2 = workbook.add_format({'num_format': '#,##0.00'})
    percent = workbook.add_format({'num_format': '0.0%'})

    worksheet = workbook.add_worksheet()

    # set column width
    worksheet.set_column('A:A', 38.0)
    worksheet.set_column('B:B', 5.40)
    worksheet.set_column('C:C', 1.5)
    worksheet.set_column('D:D', 11.5)
    worksheet.set_column('E:E', 1.5)
    worksheet.set_column('F:F', 11.5)
    worksheet.set_column('G:G', 1.5)

    # write the spread sheet
    worksheet.merge_range('A1:F1', 'Cumulative Returns', merge_format)
    worksheet.merge_range('A2:F2', 'Outvest Portfolio', merge_format)
    worksheet.merge_range('A3:F3', 'Annual Returns', merge_format)
    worksheet.write(8, 0, 'Annual Returns')

    # write year
    worksheet.write(6, 1, 'Year', bold_center)
    for i in range(len(spxt_annual_returns) - 1):
        worksheet.write(8 + i, 1, 2003 + i)

    # write SPXT
    worksheet.write(6, 3, 'SPX', bold_center)
    for i in range(len(spxt_annual_returns) - 1):
        worksheet.write(8 + i, 3, spxt_annual_returns.iloc[i + 1] / 100, percent)

    worksheet.write(9 + len(spxt_annual_returns), 0,
                    'Cumulative Returns Through ' + ticker_portfolio_returns_df.index[-1].strftime('%m/%d/%Y'))
    worksheet.write(9 + len(spxt_annual_returns), 3, spxt_cumulative_returns.iloc[-1] - 1, percent)

    worksheet.write(11 + len(spxt_annual_returns), 0, '1/1/2003 - 12/31/2009')
    worksheet.write(11 + len(spxt_annual_returns), 3, spxt_first_half_index.iloc[-1] - 1, percent)
    worksheet.write(13 + len(spxt_annual_returns), 0, '12/31/2009 - ' + ticker_portfolio_returns_df.index[-1].strftime('%m/%d/%Y'))
    worksheet.write(13 + len(spxt_annual_returns), 3, spxt_second_half_index.iloc[-1] - 1, percent)

    # write Outvest Portfolio Returns
    worksheet.write(6, 5, 'Outvest', bold_center)
    for i in range(len(spxt_annual_returns) - 1):
        worksheet.write(8 + i, 5, strategy_annual_returns.iloc[i + 1] / 100, percent)
    worksheet.write(9 + len(spxt_annual_returns), 5, strategy_cumulative_returns.iloc[-1] - 1, percent)

    worksheet.write(11 + len(spxt_annual_returns), 5, strategy_ret_first_half_index.iloc[-1] - 1, percent)
    worksheet.write(13 + len(spxt_annual_returns), 5, strategy_ret_second_half_index.iloc[-1] - 1, percent)

    workbook.close()

    print '\nRun Time: ', float(time.clock() - start_time) / 60.0, "minutes"
    print "Annual and Cumulative Return Spread Sheet Prepared"


if __name__ == '__main__':

    print 'Run back test.'
    start_time = time.clock()

    # Set Path
    path = 'P:\\Python Scripts\\Simulation_IC_Option_4'
    data_path = 'P:\\Python Scripts\\Simulation_IC_Option_4\\data'
    output_path = 'P:\\Python Scripts\\Simulation_IC_Option_4'
    os.chdir(path)

    # output file names
    filename_annual_cumulative = 'Cumulative Return.xlsx'

    # Create tday, holiday and datelist
    start_date_string = '2002-12-31'
    end_date_string = '2018-07-31'

    holiday_list, tday, datelist = trading_calendar(start_date_string, end_date_string)
    calendar_datelist = pd.date_range('2003-01-01', end_date_string)

    # Function paramics
    dynamic_indicator = True
    dynamic_second_order_indicator = True
    outvest_threshold = -0.20
    invest_threshold = 0.10
    SecondOrderTest = 0.02

    # Load Data
    # SALES_REV_TURN_df, SPY_WEIGHTS_CONSTITUENTS_df, buybacks_bulk_df, marketcap_df, currency_rate_df, \
    # adj_close_prices_df, tickers_custom_industry_groups_df, outvested_GICS_df, spxt_returns_df = load_data_local(data_path)

    master_SPY_WEIGHTS_CONSTITUENTS_df_pivot, \
    ticker_daily_return_df_pivot, \
    tickers_custom_industry_groups_df, \
    outvested_GICS_df, \
    spxt_returns_df, \
    acquisitions_df, \
    SALES_REV_TURN_df = load_data_sql()

    # Load Signals from Database
    # quant_signal_df_pivot, LRG_second_order_df_pivot, buyback_signal_df_pivot = load_signals_local(data_path)
    quant_signal_df_pivot, LRG_second_order_df_pivot, buyback_signal_df_pivot = load_signals_sql()

    # Do a ticker check
    for ticker in master_SPY_WEIGHTS_CONSTITUENTS_df_pivot.columns:
        if ticker not in tickers_custom_industry_groups_df.index:
            print 'Ticker {} Needs Mapping.'.format(ticker)
            sys.exit()

    # Data Preprocessing
    ticker_GICS_mapping_pivot = data_preprocessing(tickers_custom_industry_groups_df,
                                                   outvested_GICS_df,
                                                   master_SPY_WEIGHTS_CONSTITUENTS_df_pivot,
                                                   tday)

    # First Order Table
    first_order_indicator_df = first_order_decision_table(quant_signal_df_pivot,
                                                          buyback_signal_df_pivot,
                                                          output_path,
                                                          outvest_threshold,
                                                          invest_threshold)

    # Second Order Table
    ticker_signal_status_df = second_order_decision_table(first_order_indicator_df,
                                                          LRG_second_order_df_pivot,
                                                          ticker_GICS_mapping_pivot,
                                                          output_path,
                                                          dynamic_second_order_indicator,
                                                          SecondOrderTest)

    # Biotechnology and Defense are always in
    bio_def_list = [ticker for ticker in tickers_custom_industry_groups_df.index
                    if tickers_custom_industry_groups_df.loc[
                        ticker, 'GICS_INDUSTRY_GROUP_NAME'] == 'Capital Goods - Defense & Govt']
    bio_def_list.extend([ticker for ticker in tickers_custom_industry_groups_df.index
                         if tickers_custom_industry_groups_df.loc[
                             ticker, 'GICS_INDUSTRY_GROUP_NAME'] == 'Biotechnology'])

    bio_def_list = [ticker for ticker in bio_def_list if ticker in ticker_signal_status_df.columns]

    ticker_signal_status_df[bio_def_list] = 1.0

    # Acquisitions:
    # Names in advantaged groups invested for 8 full calendar quarters after completion date
    # Names in disadvantaged groups outvested for 8 full quarters after completion date

    missing_list = [i for i in acquisitions_df['Acquirer Ticker'].unique() if i not in list(ticker_GICS_mapping_pivot.columns)]

    ticker_signal_status_working_df = ticker_signal_status_df.copy()

    acquisitions_df = acquisitions_df.sort_values(['Acquirer Ticker', 'Announcement Date', 'Completion Date'], ascending = [True, True, True])
    acquisitions_df = acquisitions_df.reset_index().drop('index', 1)

    tickers_in_blackout_df = pd.DataFrame(columns=['Ticker','Announcement Date','Completion Date','second_qtr_announcement_dt','ic_range_end_dt'])

    #Base Section to determine blackout period end.
    #subsequent processing of resultant csv required.

    for idx, k in enumerate(acquisitions_df.index,start=1):

        ticker = acquisitions_df.loc[k, 'Acquirer Ticker']

        print idx, ticker

        tickers_in_blackout_df.loc[k,'Ticker'] = ticker

        completion_dt = acquisitions_df.loc[k, 'Completion Date']

        tickers_in_blackout_df.loc[k, 'Completion Date'] = completion_dt

        announcement_dt = acquisitions_df.loc[k, 'Announcement Date']

        tickers_in_blackout_df.loc[k, 'Announcement Date'] = announcement_dt

        if pd.isnull(acquisitions_df.loc[k, 'Completion Date']) or acquisitions_df.loc[k, 'Completion Date'] > pd.to_datetime('2018-07-31'):

            continue

        SALES_REV_TURN_ticker_df = SALES_REV_TURN_df[SALES_REV_TURN_df['Ticker'] == acquisitions_df.loc[k, 'Acquirer Ticker']]

        if SALES_REV_TURN_ticker_df.empty:

            continue

        SALES_REV_TURN_ticker_df = SALES_REV_TURN_ticker_df.set_index('PERIOD_END_DT')

        SALES_REV_TURN_ticker_df = SALES_REV_TURN_ticker_df[SALES_REV_TURN_ticker_df.index >= completion_dt]

        if len(SALES_REV_TURN_ticker_df.index) == 1 or SALES_REV_TURN_ticker_df.empty:

            continue

        SALES_REV_TURN_ticker_df = SALES_REV_TURN_ticker_df.sort_index()

        tickers_in_blackout_df.loc[k, 'second_qtr_announcement_dt'] = SALES_REV_TURN_ticker_df.iloc[1, 2] + 2 * tday

        tickers_in_blackout_df.loc[k, 'ic_range_end_dt'] = SALES_REV_TURN_ticker_df.iloc[1, 2] + 2 * tday + offsets.DateOffset(months=24)

        # tickers_in_blackout_df = tickers_in_blackout_df[tickers_in_blackout_df['ic_range_end_dt'] > pd.to_datetime('2018-07-31')]

        tickers_in_blackout_df.to_csv('tickers_in_blackout.csv')

    for j in acquisitions_df.index:

        ticker = acquisitions_df.loc[j,'Acquirer Ticker']

        completion_dt = acquisitions_df.loc[j, 'Completion Date']

        announcement_dt = acquisitions_df.loc[j, 'Announcement Date']

        # print ticker, completion_dt

        #if there is no set completion date or the completion date is outside the scenario range, continue

        if pd.isnull(acquisitions_df.loc[j, 'Completion Date']) or acquisitions_df.loc[j, 'Completion Date'] > pd.to_datetime('2018-07-31'):

            mask = (ticker_GICS_mapping_pivot.index >= announcement_dt) & (ticker_GICS_mapping_pivot.index <= pd.to_datetime('2018-07-31'))
            temp_df = ticker_GICS_mapping_pivot.loc[mask]
            temp_df = temp_df[[ticker]]
            ticker_signal_status_working_df.update(temp_df)

            continue

        #if the completion date is within the scenario range:
        #get revenue reporting/announcement date (SALES_REV_TURN_df) data for acquirer and get only the
        #history past the completion date

        SALES_REV_TURN_ticker_df = SALES_REV_TURN_df[SALES_REV_TURN_df['Ticker'] == acquisitions_df.loc[j, 'Acquirer Ticker']]

        SALES_REV_TURN_ticker_df = SALES_REV_TURN_ticker_df.set_index('PERIOD_END_DT')

        SALES_REV_TURN_ticker_df = SALES_REV_TURN_ticker_df[SALES_REV_TURN_ticker_df.index >= completion_dt]

        #If the completion date is within the scenario range but there are one or fewer revenue announcements
        #until scenario period end, IG decision from completion date to scenario end date

        if SALES_REV_TURN_ticker_df.empty or len(SALES_REV_TURN_ticker_df.index) == 1 :

            mask = (ticker_GICS_mapping_pivot.index >= announcement_dt) & (ticker_GICS_mapping_pivot.index <= pd.to_datetime('2018-07-31'))
            temp_df = ticker_GICS_mapping_pivot.loc[mask]
            temp_df = temp_df[[ticker]]
            ticker_signal_status_working_df.update(temp_df)

        #If there are 2 or more revenue announcements after completion date

        elif len(SALES_REV_TURN_ticker_df.index) > 1:

            #sort the announcements in ascending order by period end date

            SALES_REV_TURN_ticker_df = SALES_REV_TURN_ticker_df.sort_index()

            #If the announcement of the second period is outside of scenario period
            #IG decision from completion date to end of scenario

            if SALES_REV_TURN_ticker_df.iloc[1, 2] + 2 * tday > pd.to_datetime('2018-07-31'):

                mask = (ticker_GICS_mapping_pivot.index >= announcement_dt) & (ticker_GICS_mapping_pivot.index <= pd.to_datetime('2018-07-31'))
                temp_df = ticker_GICS_mapping_pivot.loc[mask]
                temp_df = temp_df[[ticker]]
                ticker_signal_status_working_df.update(temp_df)

            # if the completion date and the second revenue announcement date are both within scenario:
            #determine if 8 full quarters of acquirer revenue announcements after completion date are in the
            #scenario range:


            elif SALES_REV_TURN_ticker_df.iloc[1, 2] + 2 * tday < pd.to_datetime('2018-07-31'):

                second_period_ann_dt = SALES_REV_TURN_ticker_df.iloc[1, 2] + 2 * tday

                ic_range_end_dt = second_period_ann_dt + offsets.DateOffset(months=24)

                #if no.  IG decision from completion date to scenario end

                if ic_range_end_dt > pd.to_datetime('2018-07-31'):
                    mask = ((ticker_GICS_mapping_pivot.index >= announcement_dt) & (ticker_GICS_mapping_pivot.index <= pd.to_datetime('2018-07-31')))
                    temp_df = ticker_GICS_mapping_pivot.loc[mask]
                    temp_df = temp_df[[ticker]]
                    ticker_signal_status_working_df.update(temp_df)

                    # if yes.  IG decision from completion date to ic_range_end date

                if ic_range_end_dt <= pd.to_datetime('2018-07-31'):
                    mask = ((ticker_GICS_mapping_pivot.index >= announcement_dt) & (ticker_GICS_mapping_pivot.index <=ic_range_end_dt))
                    temp_df = ticker_GICS_mapping_pivot.loc[mask]
                    temp_df = temp_df[[ticker]]
                    ticker_signal_status_working_df.update(temp_df)


        # ticker_signal_status_working_df.loc[completion_dt:ic_range_end_dt, ticker] = temp_df.loc[completion_dt:ic_range_end_dt, ticker]

    ticker_signal_status_df = ticker_signal_status_working_df.copy()



        # print 'hi'


    # Convert Signal to Portfolio Holding (shift by 2 days)
    ticker_investment_status_df = investment_status_table_create(ticker_signal_status_df, output_path)

    # Calculate Portfolio Return
    ticker_portfolio_returns_df = portfolio_returns_calculation(ticker_investment_status_df,
                                                                ticker_daily_return_df_pivot,
                                                                master_SPY_WEIGHTS_CONSTITUENTS_df_pivot,
                                                                output_path)

    # print (ticker_portfolio_returns_df['portfolio_daily_return'] + 1).cumprod() - 1

    # # This create the Invested or Outvested Ticker List with Grossed Up Weights for ThirdPoint

        #Change '1' to '0' for outvests in line 1023 and rename output file line 1036

    # tmp = (ticker_investment_status_df == 1).astype('float') * master_SPY_WEIGHTS_CONSTITUENTS_df_pivot
    # tmp = tmp.replace(0.0, np.nan)
    # tmp = tmp.div(tmp.sum(1), axis=0)
    # tmp = tmp.reindex(datelist)
    # # tmp.sum(1)  # Check Sum Up to 1.0
    # tmp.reset_index(inplace=True)
    # tmp = tmp.rename(columns={'index': 'Date'})
    #
    # a = tmp.melt(id_vars='Date', value_vars=tmp.columns[1:], var_name='Ticker', value_name='Weight')  # Melt down to 3 columns
    # a.dropna(inplace=True)
    # a = a[(a['Date'] <= pd.to_datetime('20170331')) & (pd.to_datetime('20100101') <= a['Date'])]  # Select Date Range
    # a = a.sort_values(['Ticker', 'Date'], ascending=True)
    # a = a.rename(columns={'Weight': 'Gross Up Weight'})
    # a.to_csv('P:\\Invested Portfolio Grossed Up Weights.csv', index=False)  # Output

    # # Generate Transaction Table
    # transaction_table(master_SPY_WEIGHTS_CONSTITUENTS_df_pivot, ticker_signal_status_df, ticker_GICS_mapping_pivot,
    #                   quant_signal_df_pivot, LRG_second_order_df_pivot, buyback_signal_df_pivot, output_path)

    # Calculate Return and Write to an Excel
    annual_and_cumulative_returns(ticker_portfolio_returns_df, spxt_returns_df, output_path, filename_annual_cumulative)

    print 'Finish back test. \nRun time ', float(time.clock() - start_time) / 60.0, 'minutes'