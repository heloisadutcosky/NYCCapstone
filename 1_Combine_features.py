import pandas as pd
import numpy as np
import os



########################################################################
# Combine equity
files = os.listdir('./data/raw/CSV/Equity')

# Read First
name_ = './data/raw/CSV/Equity/' + files[0]
col_name = files[0].split()[0]
equities = pd.read_csv(name_, parse_dates=True).iloc[:,[0,1]]
equities.columns = ['date', col_name]
equities.set_index('date', inplace=True)

# Loop through the rest
for i in range(1, len(files)):
    name_ = './data/raw/CSV/Equity/' + files[i]
    col_name = files[i].split()[0]
    tmp_ = pd.read_csv(name_, parse_dates=True).iloc[:,[0,1]]
    tmp_.columns = ['date', col_name]
    tmp_.set_index('date', inplace=True)
    equities = equities.join(tmp_, how='outer' )
    
# Cut starting from 12-31-1999
equities.index = pd.to_datetime(equities.index)
equities = equities.loc['12-31-1999':,:]

# Check na
equities.isna().sum()

# Save file
equities.to_csv('./data/raw/equities.csv')



########################################################################
# Combine commodities
files = os.listdir('./data/raw/CSV/Commodities')

# Read First
name_ = './data/raw/CSV/Commodities/' + files[0]
col_name = files[0].split()[0]
comm = pd.read_csv(name_, parse_dates=True).iloc[:,[0,1]]
comm.columns = ['date', col_name]
comm.set_index('date', inplace=True)

# Loop through the rest
for i in range(1, len(files)):
    name_ = './data/raw/CSV/Commodities/' + files[i]
    col_name = files[i].split()[0]
    tmp_ = pd.read_csv(name_, parse_dates=True).iloc[:,[0,1]]
    tmp_.columns = ['date', col_name]
    tmp_.set_index('date', inplace=True)
    comm = comm.join(tmp_, how='outer' )
    
# Cut starting from 12-31-1999
comm.index = pd.to_datetime(comm.index)
comm = comm.loc['12-31-1999':,:]

# Check na
comm.isna().sum()

# Drop LMEX
comm.drop('LMEX', axis=1, inplace=True)

# Save file
comm.to_csv('./data/raw/comm.csv')



########################################################################
# Combine fixed income indices
df = pd.read_csv('./data/raw/CSV/FixIncome/indices.csv')

df.set_index('Dates', inplace=True)
df.columns = [x.split()[0] for x in df.columns.values]


# Cut starting from 12-31-1999
df.index = pd.to_datetime(df.index)
df = df.loc['12-31-1999':,:]

# Check na
df.isna().sum()

# Save file
df.to_csv('./data/raw/fi_ind.csv')


########################################################################
# Combine fixed income yield curve


df = pd.read_csv('./data/raw/CSV/FixIncome/USYieldCurve.csv')


df.set_index('Dates', inplace=True)
df.columns = [x.split()[0] for x in df.columns.values]


# Cut starting from 12-31-1999
df.index = pd.to_datetime(df.index)
df = df.loc['12-31-1999':,:]

# Check na
df.isna().sum()

# Drop USG1M
df.dropna(axis=1, inplace=True)


# Save file
df.to_csv('./data/raw/fi_yield_curve.csv')


########################################################################
# Combine market fundamentals


df = pd.read_csv('./data/raw/CSV/Fundamentals/market_fundamentals.csv')


df.set_index('Dates', inplace=True)


# Cut starting from 12-31-1999
df.index = pd.to_datetime(df.index)
df = df.loc['12-31-1999':,:]

# Check na
df.isna().sum()

# Drop missing columns
df.dropna(axis=1, inplace=True)


# Save file
df.to_csv('./data/raw/fundamentals.csv')



########################################################################
# Combine implied volatility

df = pd.read_csv('./data/raw/CSV/ImpVol/Implied_Vol.csv')


df.set_index('Dates', inplace=True)


# Cut starting from 12-31-1999
df.index = pd.to_datetime(df.index)
df = df.loc['12-31-1999':,:]

# Check na
df.isna().sum()

# Save file
df.to_csv('./data/raw/impl_vol.csv')



########################################################################
# Combine Macro 

files = os.listdir('./data/raw/CSV/Macro')

# Read First
name_ = './data/raw/CSV/Macro/' + files[0]
col_name = files[0].split()[0]
mac = pd.read_csv(name_, parse_dates=True).iloc[:,[0,1]]
mac.columns = ['date', col_name]
mac.set_index('date', inplace=True)
mac.dropna(inplace=True) # Remove nan artefacts


# Loop through the rest
for i in range(1, len(files)):
    name_ = './data/raw/CSV/Macro/' + files[i]
    col_name = files[i].split()[0]
    tmp_ = pd.read_csv(name_, parse_dates=True).iloc[:,[0,1]]
    tmp_.columns = ['date', col_name]
    tmp_.set_index('date', inplace=True)
    tmp_.dropna(inplace=True) # Remove nan artefacts
    mac = mac.join(tmp_, how='outer' )

# Fill forward
mac = mac.fillna(method='ffill')
    
# Cut starting from 12-31-1999
mac.index = pd.to_datetime(mac.index)
mac = mac.loc['12-31-1999':,:]
mac.dropna(inplace=True)

# Check na
mac.isna().sum()

# Save file
mac.to_csv('./data/raw/macro.csv')


########################################################################
# Combine VIX

df = pd.read_csv('./data/raw/CSV/VIX/vix.csv')


df = df.iloc[:,[0,1]]
df.columns = ['Dates','VIX']


df.set_index('Dates', inplace=True)



# Cut starting from 12-31-1999
df.index = pd.to_datetime(df.index)
df = df.loc['12-31-1999':,:]

# Check na
df.isna().sum()

# Save file
df.to_csv('./data/raw/vix.csv')


