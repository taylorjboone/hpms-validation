import pandas as pd
range_list=[(0,1),(0.9,2),(1.5,3)]


class Overlapping():
    def __init__(self, df):
        pass
        # self.df = pd.read_csv(df, sep='|')
        # self.df = self.df.sort_values(['RouteID', 'BeginPoint'])

    def feeder_function(self):
        df = self.df
        rids = []
        mp_list={}

        for rid in df['RouteID'].unique().tolist():
            rids.append(rid)
        # print(rids)

        for rid in rids:
            mp_list[rid] = []
            temp = df[df['RouteID'] == rid]
            # TODO Make map function instead of iterating
            for index,line in temp.iterrows():
                mp_list[line['RouteID']].append((line['BeginPoint'],line['EndPoint']))
        return mp_list

    def overlapCheck(self,):
        df=self.df
        mp_list=self.feeder_function()
        # print(mp_list)
        # TODO Get rid of redundant checks
        for k,v in mp_list.items():
            for j in v:
                for i in v:
                    if i[0] < j[0] < i[1]:
                        return df['Overlap']=='True'
                    else:
                        return df['Overlap']=='False'
                    
                    
                    
                    
                    #     print(k,': ', i[0], '<', j[0], '<', i[1],'overlap?')
                    # else:
                    #     print(k,': ', i[0], '<', j[0], '<', i[1],'No overlap?')
        

      

# meh=Overlapping(r'C:\Users\e104200\Downloads\DataItem2_Urban_ID (1).csv')