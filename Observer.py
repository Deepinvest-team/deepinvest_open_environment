import numpy  as np
import pandas as pd

import time
from datetime import datetime,date
from Exchange import Exchanges
class Observers:
    def __init__(self,window_size,train = True):
      self.init_balance   = 100000
      self.exchange  = Exchanges(self.init_balance,train,window_size)
      self.log_order = pd.DataFrame(columns=['Timestamp','Order'])
      self.window    = window_size
      if self.window <= 1 :
        self.window  = 1
      self.timestep  = self.window
      self.setuptime = False
      self.set_InitValue()
      

    def set_InitValue(self):
      order = []
      for i in range(self.window):
          self.step(order)
      self.timestep  = self.window
      self.setuptime = True

    def getDataset(self):
      return self.exchange.getDataset()
      
    def getCodeAsset(self):
      return self.exchange.getCodeAsset()

    def getLogPortion(self):
      return self.exchange.getLogPortion()

    def getLogPosition(self):
      return self.exchange.getLogPosition()

    def getLogWallet(self):
      return self.exchange.getLogWallet()

    def getPosition(self):
      return self.exchange.getPosition()

    def getCashBalance(self):
      return self.exchange.getCashBalance()

    def getNetAssetValue(self):
      return self.exchange.getNetAssetValue()
    def getLogOrder(self):
      return self.log_order

    def getWindowPrice(self):
      return self.exchange.getWindowPrice(self.timestep,self.window)

    def save_csv(self):
      self.exchange.getLogPortion().to_csv('log_portion.csv',index=False)
      self.exchange.getLogPosition().to_csv('log_position.csv',index=False)
      self.exchange.getLogWallet().to_csv('log_wallet.csv',index=False)
      self.log_order[self.window:].to_csv('log_order.csv',index=False)



    def step(self,sent_order):
      self.timestep = self.timestep +1
      mark_price    = []
      if self.setuptime == False: 
        self.timestamp = self.exchange.getDataset()[self.exchange.getCodeAsset()[0]].index[self.timestep-self.window-1]
      else:
        self.timestamp = self.exchange.getDataset()[self.exchange.getCodeAsset()[0]].index[self.timestep-1]
      
      hisOrder_dict      = {'Timestamp':self.timestamp,'Order':[sent_order]}
      self.log_order     =  pd.concat([self.log_order , pd.DataFrame(hisOrder_dict)]).reset_index(drop=True)
      
      for mp in range(len(list(self.exchange.getDataset().keys()))):
          mark_price.append(self.exchange.getDataset()[list(self.exchange.getDataset().keys())[mp]]['close'][self.timestep-1])
      self.exchange.UpdateWallet_FirstStep(self.timestamp,'First Step',self.exchange.getCodeAsset(),mark_price)

      if self.timestep < len(self.exchange.getDataset()[list(self.exchange.getDataset().keys())[0]]):
        status   = []
        if len(sent_order) > 0:
          
          for n_position in range(len(sent_order)):
            
            if len(list(sent_order[n_position].keys())) == 2 :
              has_symbol = 0
              for symbol_in in list(sent_order[n_position].keys()):
                
                if symbol_in == 'symbol':
                  has_symbol = 1
                  for n_message in list(sent_order[n_position].keys()):
                    
                    if n_message == 'symbol':
                        buff = 0
                        for i in self.exchange.getCodeAsset():
                          if i == sent_order[n_position]['symbol']:
                            buff = 1
                        if buff == 0:
                          status.append({'Position id' : 'Invalid'  , 'Note':'Invalid!!,cannot find your symbol'})
                          break
                    elif n_message   == 'open_long':
                        status.append(self.exchange.Open_LongPosition(self.timestamp,sent_order[n_position]['symbol'],self.exchange.getDataset()[sent_order[n_position]['symbol']]['close'][self.timestep-1],sent_order[n_position][n_message]))

                    elif n_message == 'open_short':  
                        status.append(self.exchange.Open_ShortPosition(self.timestamp,sent_order[n_position]['symbol'],self.exchange.getDataset()[sent_order[n_position]['symbol']]['close'][self.timestep-1],sent_order[n_position][n_message]))

                    elif n_message == 'close_long':  
                        status.append(self.exchange.Close_LongPosition(self.timestamp,sent_order[n_position]['symbol'],sent_order[n_position][n_message],self.exchange.getDataset()[sent_order[n_position]['symbol']]['close'][self.timestep-1]))
                    
                    elif n_message == 'close_short':  
                        status.append(self.exchange.Close_ShortPosition(self.timestamp,sent_order[n_position]['symbol'],sent_order[n_position][n_message],self.exchange.getDataset()[sent_order[n_position]['symbol']]['close'][self.timestep-1]))
                    
                    elif n_message == 'close_all':  
                        status.append(self.exchange.Close_AllPosition(self.timestamp,sent_order[n_position]['symbol'],self.exchange.getDataset()[sent_order[n_position]['symbol']]['close'][self.timestep-1]))  
                    
                    else :
                        status.append({'Position id' : 'Invalid'  , 'Note':'Incorrect, Try again -- Form - ex. -> [{''symbol'': ''BTC'', open_long : 100 },{''symbol'': ''ETH'', open_short : 50 }]'})
              
              if has_symbol == 0:
                status.append({'Position id' : 'Invalid'  , 'Note':'Incorrect, Try again -- Form - ex. -> [{''symbol'': ''BTC'', open_long : 100 },{''symbol'': ''ETH'', open_short : 50 }]'})
            
            else:
              status.append({'Position id' : 'Invalid'  , 'Note':'Incorrect, Try again -->  Form - ex. -> [{''symbol'': ''BTC'', open_long : 100 },{''symbol'': ''ETH'', open_short : 50 }]'})
        
        else:
              status.append({'Position id' : 0  , 'Note':'-'})


        liquidate_status = self.exchange.UpdateWallet_FinalStep(self.timestamp,list(self.exchange.getDataset().keys()),mark_price)
        self.exchange.UpdateWallet_FirstStep(self.timestamp,'Final Step',self.exchange.getCodeAsset(),mark_price)
        Done = False
      
      else:

        for i,j in zip(list(self.exchange.getDataset().keys()),mark_price):
          self.exchange.Close_AllPosition(self.timestamp,i,j)
        liquidate_status = self.exchange.UpdateWallet_FinalStep(self.timestamp,list(self.exchange.getDataset().keys()),mark_price)
        self.exchange.UpdateWallet_FirstStep(self.timestamp,'Final Step',self.exchange.getCodeAsset(),mark_price)
        status = [{'Position id' : 'END.'  , 'Note':'Out of data.'}]
        Done = True
      
      return status,self.exchange.getWindowPrice(self.timestep,self.window),liquidate_status,Done
    
    def reset(self):

      self.exchange.reset()
      self.log_order = pd.DataFrame(columns=['Timestamp','Order'])
      self.timestep  = self.window
      self.setuptime = False
      self.set_InitValue()