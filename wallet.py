import numpy  as np
import pandas as pd

import time
from datetime import datetime,date



class wallet():
  def __init__(self,init_balance,code_asset,portion_asset):
    self.CashBalance      = init_balance
    self.CodeAsset        = code_asset
    self.AssetPortion     = portion_asset
    self.NetAssetValue    = self.CashBalance
    self.fee              = 0.0004 # 0.04%
    #self.fee             = 0.1 # 0.04%
    self.log_Wallet       = pd.DataFrame(columns = ['Timestamp','CashBalance','NetAssetValue'])
    self.log_Position     = pd.DataFrame(columns = ['Timestamp','Symbol','Id','EntryPrice','Type','Margin','Unit','Fee','Comment'])
    self.log_Portion      = self.create_logPortion(self.CodeAsset)
    self.Position         = pd.DataFrame(columns = ['Timestamp','Id','Symbol','EntryPrice','MarkPrice','Type','Margin','Unit','P/L','%Changes'])
    self.Position_id      = 0

  def create_logPortion(self,code_asset):
    mux = pd.MultiIndex.from_product([code_asset,['LongPosition_value','ShortPosition_value','AssetValue','NetHedgValue','Portion','Asset_Allocation']])
    product_df  = pd.DataFrame(columns=mux)
    mux = pd.MultiIndex.from_product([['Time'],['Timestamp','Step']])
    Time_df = pd.DataFrame(columns=mux)
    mux = pd.MultiIndex.from_product([['Cash'],['CashBalance','Asset_Allocation']])
    Cash_df = pd.DataFrame(columns=mux)
    return pd.concat([Time_df,Cash_df,product_df], axis=1)


  def getLogPortion(self):
    return self.log_Portion

  def getLogPosition(self):
    return self.log_Position
  
  def getLogWallet(self):
    return self.log_Wallet

  def getPosition(self):
    return self.Position

  def getCashBalance(self):
    return self.CashBalance 

  def getNetAssetValue(self):
    return self.NetAssetValue


  def Open_LongPosition(self,timestamp,symbol,price,coin):
    if self.CashBalance < 10:
      return {'Position id' : 'Invalid' , 'Note': 'Insufficiency Balance - Unable to place order due to insufficient balance.'}
    if coin < 10:
      return {'Position id' : 'Invalid' , 'Note': 'Position Size must be greater than 10 Coin.'}
    if price < 0:
      return {'Position id' : 'Invalid' , 'Note': 'Price <0'}


    if coin > self.CashBalance :
      coin = self.CashBalance

    if len(self.log_Portion) <= 0 :
      max_open = (self.AssetPortion[symbol] * self.NetAssetValue)
      if coin >= abs(max_open):
        coin   = abs(max_open)

    else:       #  25000 -.... > 10000 - 15000 = -5000 = 30000
                  # 20000 - (-15000) = 35000
      max_open = (self.AssetPortion[symbol] * self.NetAssetValue) - self.log_Portion[symbol]['NetHedgValue'].values[-1]  
      if abs(self.log_Portion[symbol]['Portion'].values[-1]) >= self.AssetPortion[symbol]:
        if self.log_Portion[symbol]['NetHedgValue'].values[-1] > 0:
          text = symbol + ' greater than or equal to '+str(self.AssetPortion[symbol] * 100)+'%. of portfolio'
          return {'Position id' : 'Invalid' , 'Note':text}
        else:
          coin =  abs(max_open)
      elif coin > abs(max_open) :
        coin =  abs(max_open)
    #update : 25/02/2022 nopanon : OverPosition
    if (coin + self.log_Portion[symbol]['LongPosition_value'].values[-1]) >= (self.AssetPortion[symbol] * self.NetAssetValue):
        max_open = (self.AssetPortion[symbol] * self.NetAssetValue) - self.log_Portion[symbol]['LongPosition_value'].values[-1]
        if max_open < 0:
            text = f'{symbol} Long Position Size greater than or equal to {round(self.AssetPortion[symbol] * self.NetAssetValue,2)}.'
            return {'Position id' : 'Invalid' , 'Note':text}
        else:
          coin =  abs(max_open)
        #text = f'{symbol} Long Position Size greater than or equal to {self.AssetPortion[symbol] * self.NetAssetValue}.'
        #return {'Position id' : 'Invalid' , 'Note':text}

    if coin < 10:
      return {'Position id' : 'Invalid' , 'Note': 'Position Size must be greater than 10 Coin.'}


    try : 
      Fee   = (coin * self.fee)
      unit  = (coin - Fee) / price 

      self.CashBalance  = self.CashBalance - coin 
      self.Position_id  = self.Position_id + 1



      #-- Add Position 
      position           = {'Timestamp': [timestamp],'Id': [self.Position_id],'Symbol': [symbol],'EntryPrice': [price],'MarkPrice': [price],'Type': ['Long'],'Margin':[coin - Fee ],'Unit': [unit],'P/L':[0],'%Changes':[0]}
      self.Position      =  pd.concat([self.Position , pd.DataFrame(position)]).reset_index(drop=True)
      
      #-- Add log_Position 
      log_position       = {'Timestamp': [timestamp],'Symbol': [symbol],'Id': [self.Position_id], 'EntryPrice': [price],'Type': ['Open_Long'],'Margin': [coin -Fee],'Unit': [unit],'Fee': [Fee],'Comment':['-']}
      self.log_Position  =  pd.concat([self.log_Position , pd.DataFrame(log_position)]).reset_index(drop=True)

      return {'Position id' : self.Position_id , 'Note':'Order placed'}

    except:
      return {'Position id' : 'Invalid' , 'Note': 'Unable to place order. !!!'}


  def Open_ShortPosition(self,timestamp,symbol,price,coin):
    if self.CashBalance < 10:
      return {'Position id' : 'Invalid' , 'Note': 'Insufficiency Balance - Unable to place order due to insufficient balance.'}
    if coin < 10:
      return {'Position id' : 'Invalid' , 'Note': 'Position Size must be greater than 10 Coin.'}
    if price < 0:
      return {'Position id' : 'Invalid' , 'Note': 'Price <0'}
    if coin > self.CashBalance :
      coin = self.CashBalance
    if len(self.log_Portion) <= 0 :
      max_open = (self.AssetPortion[symbol] * self.NetAssetValue)
      if coin >= abs(max_open):
        coin   = abs(max_open)
    else:   # 25000 -------- -10000
      max_open = -(self.AssetPortion[symbol] * self.NetAssetValue) - self.log_Portion[symbol]['NetHedgValue'].values[-1]
      if abs(self.log_Portion[symbol]['Portion'].values[-1]) > self.AssetPortion[symbol]:
        if self.log_Portion[symbol]['NetHedgValue'].values[-1] < 0:
          text = symbol + ' greater than or equal to '+str(self.AssetPortion[symbol] * 100)+'%. of portfolio'
          return {'Position id' : 'Invalid' , 'Note':text}
        else:
          coin =  abs(max_open)
      elif coin > abs(max_open) :
        coin =  abs(max_open)
    #update : 25/02/2022 nopanon : OverPosition
    if coin + abs(self.log_Portion[symbol]['ShortPosition_value'].values[-1]) >= (self.AssetPortion[symbol] * self.NetAssetValue):
      max_open = (self.AssetPortion[symbol] * self.NetAssetValue) - abs(self.log_Portion[symbol]['ShortPosition_value'].values[-1]) 
      if max_open < 0:
        text = f'{symbol} Short Position Size greater than or equal to {round(self.AssetPortion[symbol] * self.NetAssetValue,2)}.'
        return {'Position id' : 'Invalid' , 'Note':text}
      else:
        coin =  abs(max_open)

    if coin < 10:
      return {'Position id' : 'Invalid' , 'Note': 'Position Size must be greater than 10 Coin.'}

    try : 
      Fee   = (coin * self.fee)
      unit  = (coin - Fee) / price 
      
      self.CashBalance  = self.CashBalance - coin 
      self.Position_id  = self.Position_id + 1


      #-- Add Position 
      position           = {'Timestamp': [timestamp],'Id': [self.Position_id],'Symbol': [symbol],'EntryPrice': [price],'MarkPrice': [price],'Type': ['Short'],'Margin':[-coin + Fee],'Unit': [-unit],'P/L':[0],'%Changes':[0]}
      self.Position      =  pd.concat([self.Position , pd.DataFrame(position)]).reset_index(drop=True)
      
      #-- Add log_Position 
      log_position       = {'Timestamp': [timestamp],'Symbol': [symbol],'Id': [self.Position_id], 'EntryPrice': [price],'Type': ['Open_Short'],'Margin': [-coin + Fee],'Unit': [-unit],'Fee': [Fee],'Comment':['-']}
      self.log_Position  =  pd.concat([self.log_Position , pd.DataFrame(log_position)]).reset_index(drop=True)

      return {'Position id' : self.Position_id , 'Note':'Order placed'}

    except:
      return {'Position id' : 'Invalid' , 'Note': 'Unable to place order. !!!'}

  def Close_LongPosition(self,timestamp,symbol,id,price):
      try:
        position_index = self.Position[(self.Position['Id'] == id) & (self.Position['Type'] == 'Long') & (self.Position['Symbol'] == symbol)].index[0]

        unit           = self.Position['Unit'][position_index]
        magin          = self.Position['Margin'][position_index]
        value          = unit  * price
        Fee            = value * self.fee
        self.CashBalance  = self.CashBalance + value  - Fee


        #-- del Position 
        self.Position  = self.Position.drop([position_index]).reset_index(drop=True)
        
        #-- Add log_Position 
        log_position       = {'Timestamp': [timestamp],'Symbol': [symbol],'Id': [id], 'EntryPrice': [price],'Type': ['Close_Long'],'Margin': [value],'Unit': [unit],'Fee': [Fee],'Comment':['-']}
        self.log_Position  =  pd.concat([self.log_Position , pd.DataFrame(log_position)]).reset_index(drop=True)

        return {'Position id' : id , 'Note':'Order closed.'}

      except:
        return {'Position id' : 'Invalid'  , 'Note':'Unable to close order. !!!'}

  def Close_ShortPosition(self,timestamp,symbol,id,price): 
      try:
        position_index = self.Position[(self.Position ['Id'] == id) & (self.Position ['Type'] == 'Short') & (self.Position['Symbol'] == symbol)].index[0]
        unit           = self.Position['Unit'][position_index]
        magin          = self.Position['Margin'][position_index]
        value          = unit  * price
        Fee            = - value * self.fee
        self.CashBalance  = self.CashBalance + (value - magin) - magin - Fee


        #-- del Position 
        self.Position  = self.Position.drop([position_index]).reset_index(drop=True)
        
        #-- Add log_Position 
        log_position       = {'Timestamp': [timestamp],'Symbol': [symbol],'Id': [id], 'EntryPrice': [price],'Type': ['Close_Short'],'Margin': [value],'Unit': [unit],'Fee': [Fee],'Comment':['-']}
        self.log_Position  =  pd.concat([self.log_Position , pd.DataFrame(log_position)]).reset_index(drop=True)

        return {'Position id' : id , 'Note':'Order closed.'}

      except:
        return {'Position id' : 'Invalid'  , 'Note':'Unable to close order. !!!'}

  def Close_AllPosition(self,timestamp,symbol,mark_price):
    id_openPosition = self.Position['Id'][self.Position['Symbol'] == symbol].tolist()
    if len(id_openPosition) > 0 :
      id_closePositoin   = []
      note_closePosition = []
      for i in id_openPosition:
        if self.Position['Type'][self.Position['Id'] == i].values[0] == 'Long':
          result  = self.Close_LongPosition(timestamp,symbol,i,mark_price)
        elif self.Position['Type'][self.Position['Id'] == i].values[0] == 'Short':
          result  = self.Close_ShortPosition(timestamp,symbol,i,mark_price)
        id_closePositoin.append(result['Position id'])
        note_closePosition.append(result['Note'])
      status = {'Position id' : id_closePositoin  , 'Note': note_closePosition}
    else:
      status = {'Position id' : 'Invalid'  , 'Note':'Total Position = 0'}
    return status

  def Liquidate_position(self,timestamp,symbol,price):
    status = []
    for i in range(len(self.Position)):
      if ((abs(self.Position['Margin'][i]) + self.Position['P/L'][i])  <= 0 )and  (self.Position['Symbol'][i] == symbol)and  (self.Position['Type'][i] == 'Long'):
          position_index = i
          id             = self.Position['Id'][position_index]
          unit           = self.Position['Unit'][position_index]
          # update nopanon 19012022
          #magin          = self.Position['Margin'][position_index]
          magin          = 0
          value          = unit  * price
          Fee            = 0
          #self.CashBalance  = self.CashBalance + value  - Fee

          #-- del Position 
          self.Position  = self.Position.drop([position_index])
          
          #-- Add log_Position 
          log_position       = {'Timestamp': [timestamp],'Symbol': [symbol],'Id': [id], 'EntryPrice': [price],'Type': ['Close_Long'],'Margin': [magin],'Unit': [unit],'Fee': [Fee],'Comment':['Liquidate']}
          self.log_Position  =  pd.concat([self.log_Position , pd.DataFrame(log_position)]).reset_index(drop=True)

          status.append({'Position id' : id , 'Note':'Liquidate_position'})
      elif (( abs(self.Position['Margin'][i]) + self.Position['P/L'][i] )  <= 0) and  (self.Position['Symbol'][i] == symbol )and  (self.Position['Type'][i] == 'Short'):
          position_index = i
          id             = self.Position['Id'][position_index]
          unit           = self.Position['Unit'][position_index]
          #magin          = self.Position['Margin'][position_index] * 2
          # update nopanon 19012022
          magin          = self.Position['Margin'][position_index] * 2
          value          = unit  * price
          Fee            = 0
          #self.CashBalance  = self.CashBalance + (value - magin) - magin - Fee


          #-- del Position 
          self.Position  = self.Position.drop([position_index])
          
          #-- Add log_Position 
          log_position       = {'Timestamp': [timestamp],'Symbol': [symbol],'Id': [id], 'EntryPrice': [price],'Type': ['Close_Short'],'Margin': [magin],'Unit': [unit],'Fee': [Fee],'Comment':['Liquidate']}
          self.log_Position  =  pd.concat([self.log_Position , pd.DataFrame(log_position)]).reset_index(drop=True)
    
          status.append({'Position id' : id , 'Note':'Liquidate_position'})

    self.Position  = self.Position.reset_index(drop=True)
    return status

  def UpdateWallet_FinalStep(self,timestamp,symbol_list,price_list):
    #-- update position
    sum_margin = []
    for i in range(len(symbol_list)):
      self.Position.loc[self.Position['Symbol'] == symbol_list[i] , 'MarkPrice']  = [price_list[i]] * len(self.Position[self.Position['Symbol'] == symbol_list[i]]) 
      self.Position.loc[(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i] ) , 'P/L']  =  (self.Position['MarkPrice'][(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i])]  - self.Position['EntryPrice'][(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i] )])  * self.Position['Unit'][(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i] )] 
      self.Position.loc[(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i] ) , 'P/L']  =  (self.Position['MarkPrice'][(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i])]  - self.Position['EntryPrice'][(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i] )])  * self.Position['Unit'][(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i] )]
      self.Position.loc[(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i] ) , '%Changes']  =  ( self.Position['P/L'][(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i] )])  / self.Position['Margin'][(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i] )]  * 100 
      self.Position.loc[(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i] ) , '%Changes']  =  ( self.Position['P/L'][(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i] )])  / abs(self.Position['Margin'][(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i] )]) * 100 
      liquidate_status   = self.Liquidate_position(timestamp,symbol_list[i],price_list[i])
      #update : 25/02/2022 nopanon : remove liq
      #liquidate_status   = []
      sum_margin.append(self.Position['Margin'][(self.Position['Symbol'] == symbol_list[i])&(self.Position['Type'] == 'Long')].sum() + abs(self.Position['Margin'][(self.Position['Symbol'] == symbol_list[i])&(self.Position['Type'] == 'Short')].sum()) + self.Position['P/L'][self.Position['Symbol'] == symbol_list[i]].sum())
    self.NetAssetValue = self.CashBalance + np.array(sum_margin).sum()
    #-- Add log_Wallet 
    log_wallet         = {'Timestamp': [timestamp],'CashBalance': [self.CashBalance], 'NetAssetValue': [self.NetAssetValue]}
    self.log_Wallet    =  pd.concat([self.log_Wallet , pd.DataFrame(log_wallet)]).reset_index(drop=True)
    return liquidate_status

  def UpdateWallet_FirstStep(self,timestamp,step,symbol_list,price_list):
    #-- update position
    sum_margin = []
    for i in range(len(symbol_list)):
      self.Position.loc[self.Position['Symbol'] == symbol_list[i] , 'MarkPrice']  = [price_list[i]] * len(self.Position[self.Position['Symbol'] == symbol_list[i]]) 
      self.Position.loc[(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i] ) , 'P/L']  =  (self.Position['MarkPrice'][(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i])]  - self.Position['EntryPrice'][(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i] )])  * self.Position['Unit'][(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i] )] 
      self.Position.loc[(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i] ) , 'P/L']  =  (self.Position['MarkPrice'][(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i])]  - self.Position['EntryPrice'][(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i] )])  * self.Position['Unit'][(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i] )]
      self.Position.loc[(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i] ) , '%Changes']  =  ( self.Position['P/L'][(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i] )])  / self.Position['Margin'][(self.Position['Type'] == 'Long' ) & (self.Position['Symbol'] == symbol_list[i] )]  * 100 
      self.Position.loc[(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i] ) , '%Changes']  =  ( self.Position['P/L'][(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i] )])  / abs(self.Position['Margin'][(self.Position['Type'] == 'Short') & (self.Position['Symbol'] == symbol_list[i] )])  * 100 
      sum_margin.append(self.Position['Margin'][(self.Position['Symbol'] == symbol_list[i])&(self.Position['Type'] == 'Long')].sum() + abs(self.Position['Margin'][(self.Position['Symbol'] == symbol_list[i])&(self.Position['Type'] == 'Short')].sum()) + self.Position['P/L'][self.Position['Symbol'] == symbol_list[i]].sum())
    self.NetAssetValue = self.CashBalance + np.array(sum_margin).sum()
    
    #-- ***** Add log_Portion 
    #-- Add time
    time_dict         = {('Time', 'Timestamp'): [timestamp],('Time', 'Step'): [step]}
    time_df           = pd.DataFrame(time_dict)


    #-- Add cash
    cash_dict         = {('Cash', 'Asset_Allocation'): [self.CashBalance / self.NetAssetValue],('Cash', 'CashBalance'): [self.CashBalance]}
    cash_df           = pd.DataFrame(cash_dict)
    #-- Add asset 
    asset_list = []
    for j in range(len(symbol_list)):
      long_value         = self.Position['Margin'][ (self.Position['Symbol'] == symbol_list[j]) & (self.Position['Type'] == 'Long' )].sum()  + self.Position['P/L'][ (self.Position['Symbol'] == symbol_list[j]) & (self.Position['Type'] == 'Long' )].sum()
      short_value        = self.Position['Margin'][ (self.Position['Symbol'] == symbol_list[j]) & (self.Position['Type'] == 'Short' )].sum() - self.Position['P/L'][ (self.Position['Symbol'] == symbol_list[j]) & (self.Position['Type'] == 'Short' )].sum()
      hedg_asset         = long_value + short_value
      value_asset        = self.Position['Margin'][(self.Position['Symbol'] == symbol_list[j])&(self.Position['Type'] == 'Long')].sum() + abs(self.Position['Margin'][(self.Position['Symbol'] == symbol_list[j])&(self.Position['Type'] == 'Short')].sum()) + self.Position['P/L'][self.Position['Symbol'] == symbol_list[j]].sum()

      asset_list.append({(symbol_list[j], 'LongPosition_value'): [long_value],(symbol_list[j], 'ShortPosition_value'): [short_value],(symbol_list[j], 'AssetValue'): [value_asset],(symbol_list[j], 'NetHedgValue'): [hedg_asset],(symbol_list[j], 'Portion'): [abs(hedg_asset / self.NetAssetValue)],(symbol_list[j], 'Asset_Allocation'): [abs(value_asset / self.NetAssetValue)]})
    if len(asset_list) <= 1 :
      asset_df            = pd.DataFrame(asset_list[0])
    else:
      asset_df            = pd.DataFrame(asset_list[0])
      for i in range(1,len(asset_list)):
        asset_df      =  pd.concat([asset_df,pd.DataFrame(asset_list[i])], axis=1)

    log_portion_df      =  pd.concat([time_df,cash_df,asset_df], axis=1)
    self.log_Portion    =  pd.concat([self.log_Portion , log_portion_df])
    self.log_Portion    =  self.log_Portion.fillna(value=0.).reset_index(drop=True)
   




  def reset(self,init_balance):
    self.CashBalance      = init_balance
    self.NetAssetValue    = self.CashBalance
    self.fee              = 0.0004 # 0.04%
    self.log_Wallet       = pd.DataFrame(columns = ['Timestamp','CashBalance','NetAssetValue'])
    self.log_Position     = pd.DataFrame(columns = ['Timestamp','Symbol','Id','EntryPrice','Type','Margin','Unit','Fee','Comment'])
    self.log_Portion      = self.create_logPortion(self.CodeAsset)
    self.Position         = pd.DataFrame(columns = ['Timestamp','Id','Symbol','EntryPrice','MarkPrice','Type','Margin','Unit','P/L','%Changes'])
    self.Position_id      = 0