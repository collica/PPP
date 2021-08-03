from xbbg import blp
import quandl
from bs4 import BeautifulSoup
import wbgapi as wb
import pandas as pd
from Keys import *
import json
import numpy as np
import requests
import csv

################## UPDATE TO MOST RECENT OBSERVATION ##################

BigMac_Date = "2021-01-31" # Most recent half year ending 1/31 or 7/31
IMF_Date = "2021-12-31" # Year end of current year

#######################################################################

quandl.ApiConfig.api_key = QuandlKey

### OECD item may change each year! ###
universe = {'DEU': {'Code': 'EUR', 'Spot': 'USDEUR BGN Curncy', 'Consumer': 'BPPPCPEU INDEX', 'Producer': 'BPPPPPEU INDEX', 'BigMac': 'ECONOMIST/BIGMAC_EUR', 'IMF': 'ODA/DEU_PPPEX', 'OECD': 'EU27_2020', 'Currency': 'The Euro'},
            'GBR': {'Code': 'GBP', 'Spot': 'USDGBP BGN Curncy', 'Consumer': 'BPPPCPGB INDEX', 'Producer': 'BPPPPPGB INDEX', 'BigMac': 'ECONOMIST/BIGMAC_GBR', 'IMF': 'ODA/GBR_PPPEX', 'OECD': 'GBR', 'Currency': 'British Pound'},
            'CHE': {'Code': 'CHF', 'Spot': 'USDCHF BGN Curncy', 'Consumer': 'BPPPCPCH INDEX', 'Producer': 'BPPPPPCH INDEX', 'BigMac': 'ECONOMIST/BIGMAC_CHE', 'IMF': 'ODA/CHE_PPPEX', 'OECD': 'CHE', 'Currency': 'Swiss Franc'},
            'JPN': {'Code': 'JPY', 'Spot': 'USDJPY BGN Curncy', 'Consumer': 'BPPPCPJP INDEX', 'Producer': 'BPPPPPJP INDEX', 'BigMac': 'ECONOMIST/BIGMAC_JPN', 'IMF': 'ODA/JPN_PPPEX', 'OECD': 'JPN', 'Currency': 'Japanese Yen'},
            'AUS': {'Code': 'AUD', 'Spot': 'USDAUD BGN Curncy', 'Consumer': 'BPPPCPAU INDEX', 'Producer': 'BPPPPPAU INDEX', 'BigMac': 'ECONOMIST/BIGMAC_AUS', 'IMF': 'ODA/AUS_PPPEX', 'OECD': 'AUS', 'Currency': 'Australian Dollar'},
            'NZL': {'Code': 'NZD', 'Spot': 'USDNZD BGN Curncy', 'Consumer': 'BPPPCPNZ INDEX', 'Producer': 'BPPPPPNZ INDEX', 'BigMac': 'ECONOMIST/BIGMAC_NZL', 'IMF': 'ODA/NZL_PPPEX', 'OECD': 'NZL', 'Currency': 'New Zealand Dollar'},
            'NOR': {'Code': 'NOK', 'Spot': 'USDNOK BGN Curncy', 'Consumer': 'BPPPCPNO INDEX', 'Producer': 'BPPPPPNO INDEX', 'BigMac': 'ECONOMIST/BIGMAC_NOR', 'IMF': 'ODA/NOR_PPPEX', 'OECD': 'NOR', 'Currency': 'Norwegian Krone'},
            'SWE': {'Code': 'SEK', 'Spot': 'USDSEK BGN Curncy', 'Consumer': 'BPPPCPSE INDEX', 'Producer': 'BPPPPPSE INDEX', 'BigMac': 'ECONOMIST/BIGMAC_SWE', 'IMF': 'ODA/SWE_PPPEX', 'OECD': 'SWE', 'Currency': 'Swedish Krona'},
            'DNK': {'Code': 'DKK', 'Spot': 'USDDKK BGN Curncy', 'Consumer': 'BPPPCPDK INDEX', 'Producer': 'BPPPPPDK INDEX', 'BigMac': 'ECONOMIST/BIGMAC_DNK', 'IMF': 'ODA/DNK_PPPEX', 'OECD': 'DNK', 'Currency': 'Danish Krone'},
            'CAN': {'Code': 'CAD', 'Spot': 'USDCAD BGN Curncy', 'Consumer': 'BPPPCPCA INDEX', 'Producer': 'BPPPPPCA INDEX', 'BigMac': 'ECONOMIST/BIGMAC_CAN', 'IMF': 'ODA/CAN_PPPEX', 'OECD': 'CAN', 'Currency': 'Canadian Dollar'},
            }

#Spot, CPI and PMI via Bloomberg
def get_curr(ui_key):
    df = pd.concat([blp.bdp(universe[ui_key][t], 'PX_LAST') for t in universe[ui_key]])
    df['type'] = ['Spot', 'Consumer', 'Producer']
    df['ISO'] = ui_key
    df = df.pivot_table(index='ISO', columns='type', values='px_last').reset_index()
    return df

bb_df = pd.concat([get_curr(x) for x in universe])
bb_df.set_index('ISO')

#World Bank
WorldBank = wb.data.DataFrame(['PA.NUS.PPP'], mrv=1)
wb_df = pd.DataFrame(WorldBank)
wb_df.columns = ['World Bank']
wb_df.index.names = ['ISO']
#wb_df.to_csv(r'X:\Trading\Employee Folders\CC\Python\PPP\WorldBank.csv', index = True)

#BigMac index via Quandl
mac_list = []
for x in universe.keys():
    mac = quandl.get(universe[x]['BigMac'], start_date=BigMac_Date, end_date=BigMac_Date)
    mac = mac.iloc[0]['dollar_ppp']
    mac_list.append(mac)

#IMF estimates via Quandl
imf_list = []
for x in universe.keys():
    imf = quandl.get(universe[x]['IMF'], start_date=IMF_Date, end_date=IMF_Date)
    imf = imf.iloc[0]['Value']
    imf_list.append(imf)

#OECD
OECD_url = url = 'https://stats.oecd.org/SDMX-JSON/data/SNA_TABLE4/AUS+AUT+BEL+CAN+CHL+COL+CRI+CZE+DNK+EST+FIN+FRA+DEU+GRC+HUN+ISL+IRL+ISR+ITA+JPN+KOR+LTU+LVA+LUX+MEX+NLD+NZL+NOR+POL+PRT+SVK+SVN+ESP+SWE+CHE+TUR+GBR+USA+EA19+EU27_2020+NMEC+ARG+BRA+BGR+CHN+HRV+CYP+IND+IDN+MLT+ROU+RUS+SAU+ZAF.EXC+EXCE+PPPGDP+PPPPRC+PPPP41.CD/all?dimensionAtObservation=allDimensions&contentType=csv'
df = pd.read_csv(url)
max_year = df['Year'].max()
oecd_df = df[(df['TRANSACT'] == 'PPPGDP') & (df['Year'] == max_year)][['Year', 'LOCATION', 'Country', 'Value']].pivot_table(index=['LOCATION', 'Country'], columns='Year', values='Value').reset_index()
oecd_df.columns = ['ISO', 'Country Name', 'OECD']
#print(oecd_df)
#oecd_df.to_csv(r'X:\Trading\Employee Folders\CC\Python\PPP\OECD.csv', index = True)

#Combine sources into final price frame

name_list = []
ccy_list = []
for x in universe.keys():
    name = universe[x]['Currency']
    name_list.append(name)
    #ccy = universe[x]['Code']
    #ccy_list.append(ccy)

final_price_frame = pd.merge(bb_df, wb_df, on='ISO')
final_price_frame1 = pd.merge(final_price_frame, oecd_df, on='ISO')
final_price_frame2 = final_price_frame1.drop(columns='Country Name')
final_price_frame2['Big Mac'] = mac_list #FOR DAVE: this isnt merged on a key/column... can this come in out of order?
final_price_frame2['IMF'] = imf_list #FOR DAVE: this isnt merged on a key/column... can this come in out of order?
#final_price_frame2['CCY'] = ccy_list
final_price_frame2['Currency'] = name_list
final_price_frame2 = final_price_frame2[['ISO', 'Currency', 'Spot', 'Consumer', 'Producer', 'World Bank', 'OECD', 'Big Mac', 'IMF']]

print("\nIndicative exchange rates:\n")
print(final_price_frame2)
final_price_frame2.to_csv(r'X:\Trading\Employee Folders\CC\Python\PPP\Final Prices.csv', index=True)

#Calculate indicative premium/discount, average, and hedging decision

final_calc_frame = final_price_frame2
final_calc_frame['Consumer Calc'] = (final_calc_frame['Consumer'] / final_calc_frame['Spot'] - 1).astype(float)
final_calc_frame['Producer Calc'] = (final_calc_frame['Producer'] / final_calc_frame['Spot'] - 1).astype(float)
final_calc_frame['World Bank Calc'] = (final_calc_frame['World Bank'] / final_calc_frame['Spot'] - 1).astype(float)
final_calc_frame['OECD Calc'] = (final_calc_frame['OECD'] / final_calc_frame['Spot'] - 1).astype(float)
final_calc_frame['Big Mac Calc'] = (final_calc_frame['Big Mac'] / final_calc_frame['Spot'] - 1).astype(float)
final_calc_frame['IMF Calc'] = (final_calc_frame['IMF'] / final_calc_frame['Spot'] - 1).astype(float)
final_calc_frame['Average'] = final_calc_frame[['Consumer Calc', 'Producer Calc', 'World Bank Calc', 'OECD Calc', 'Big Mac Calc', 'IMF Calc']].mean(axis=1).astype(float)
final_calc_frame.sort_values(by='Average', ascending=False, inplace=True)

#Hedging decision
decision_list = []
for index, x in final_calc_frame.iterrows():
    if (x['Average']) < .1:
        decision = 'Do not hedge'
        decision_list.append(decision)
    elif .1 < (x['Average']) < .25:
        decision = 'Hedge 25%'
        decision_list.append(decision)
    elif (x['Average']) > .25:
        decision = 'Hedge 50%'
        decision_list.append(decision)

#Format table
final_calc_frame['Consumer Calc'] = round(final_calc_frame['Consumer Calc'].mul(100), 2).astype(str).add('%')
final_calc_frame['Producer Calc'] = round(final_calc_frame['Producer Calc'].mul(100), 2).astype(str).add('%')
final_calc_frame['World Bank Calc'] = round(final_calc_frame['World Bank Calc'].mul(100), 2).astype(str).add('%')
final_calc_frame['OECD Calc'] = round(final_calc_frame['OECD Calc'].mul(100), 2).astype(str).add('%')
final_calc_frame['Big Mac Calc'] = round(final_calc_frame['Big Mac Calc'].mul(100), 2).astype(str).add('%')
final_calc_frame['IMF Calc'] = round(final_calc_frame['IMF Calc'].mul(100), 2).astype(str).add('%')
final_calc_frame['Average'] = round(final_calc_frame['Average'].mul(100), 2).astype(str).add('%')
final_calc_frame['Decision'] = decision_list

final_calc_frame.drop(columns=['Spot', 'Consumer', 'Producer', 'World Bank', 'OECD', 'Big Mac', 'IMF'], axis=1, inplace=True)
final_calc_frame.rename({'Consumer Calc':'Consumer', 'Producer Calc':'Producer', 'World Bank Calc':'World Bank', 'OECD Calc':'OECD', 'Big Mac Calc':'Big Mac', 'IMF Calc':'IMF'}, axis='columns', inplace=True)
final_calc_frame = final_calc_frame[['ISO', 'Currency', 'Decision', 'Average', 'Consumer', 'Producer', 'World Bank', 'OECD', 'Big Mac', 'IMF']]

print("\nIndicative premiums/discounts:\n")
print(final_calc_frame)
final_calc_frame.to_csv(r'X:\Trading\Employee Folders\CC\Python\PPP\Final Calc.csv', index=True)

#TODO: rec discounts, set up email, set up schedule, check with dave on appending lists