import pandas as pd
import matplotlib.pyplot as plt
plt.close("all")



mpd = pd.read_csv('mpd.csv')
gdp = (
    mpd
    .query('FREQ=="Q" and REF_AREA =="U2" and OBS_STATUS == "A" and PD_ITEM == "YER"')
    [["TIME_PERIOD", "OBS_VALUE", "PD_SEAS_EX"]]
    .assign(TIME_PERIOD=lambda x: pd.PeriodIndex(x["TIME_PERIOD"], freq="Q"))
    .sort_values(by=["PD_SEAS_EX", "TIME_PERIOD"])
    .groupby("PD_SEAS_EX", group_keys=False)  
    .apply(lambda g: (
        g.tail(3)
        .assign(CUM_GROWTH=lambda x: (1 + x["OBS_VALUE"]/100).cumprod())
        .tail(1)
        .assign(YOY_GROWTH=lambda x: x["CUM_GROWTH"] - 1)
    ))
)
gdp.sort_values(by = "TIME_PERIOD",  inplace=True)
gdp.plot(y="YOY_GROWTH", x="TIME_PERIOD")


hicp = (
    mpd
    .query('FREQ=="Q" and REF_AREA =="U2" and OBS_STATUS == "A" and PD_ITEM == "HIC"')
    [["TIME_PERIOD", "OBS_VALUE", "PD_SEAS_EX"]]
    .assign(TIME_PERIOD=lambda x: pd.PeriodIndex(x["TIME_PERIOD"], freq="Q"))
    .sort_values(by=["PD_SEAS_EX", "TIME_PERIOD"])
    .groupby("PD_SEAS_EX", group_keys=False) 
    .apply(lambda g: (
        g.tail(1) 
    ))
)
hicp.sort_values(by = "TIME_PERIOD",  inplace=True)
hicp.plot(y="OBS_VALUE", x="TIME_PERIOD")

plt.show()