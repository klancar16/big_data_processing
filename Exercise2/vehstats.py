import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import datetime as DT

if __name__ == '__main__':
    dat = pd.read_csv("data/vehicle_data.csv", delimiter=';', encoding='ANSI')
    # dat = pd.read_csv("Tieliikenne AvoinData 4.10.csv", delimiter=';', encoding='ANSI')
    cars = dat[dat.ajoneuvoluokka.isin(['M1', 'M1G'])]
    cars = cars[['merkkiSelvakielinen', 'matkamittarilukema', 'ensirekisterointipvm', 'Co2']]
    # merkkiSelvakielinen - brand
    # matkamittarilukema - kms driven
    # ensirekisterointipvm - first registered

    plt.rcParams["figure.figsize"] = [16.0, 12.0]

    # top 5 brands
    group_by_brands = cars.groupby('merkkiSelvakielinen').size().sort_values(ascending=False)[:5]
    group_by_brands = group_by_brands.rename('Top 5 brands')
    plt.subplot(221)
    group_by_brands.plot.pie(autopct='%1.0f%%')
    plt.axis('equal')
    ###

    # shares of cars by km
    group_by_km = cars.groupby(pd.cut(cars['matkamittarilukema'],
                                      np.append(np.arange(0, 300001, 50000), cars['matkamittarilukema'].max()))).size()
    group_by_km = group_by_km.rename('Shares by km')
    plt.subplot(222)
    group_by_km.plot.pie(autopct='%1.0f%%')
    plt.axis('equal')
    ###

    # shares of cars by age
    register_dates = pd.to_datetime(cars['ensirekisterointipvm'], format="%Y/%m/%d")
    now = pd.Timestamp(DT.datetime.now())
    age = (now - register_dates).astype('<m8[Y]')
    group_by_age = age.groupby(
        pd.cut(age, np.concatenate((np.arange(0.0, 20.1, 5.0), [age.max()])))).size()
    group_by_age = group_by_age.rename('Shares by age')

    plt.subplot(223)
    group_by_age.plot.pie(autopct='%1.0f%%')
    plt.axis('equal')
    ###

    # shares of cars by co2
    group_by_co = cars.groupby(
        pd.cut(cars['Co2'], np.concatenate(([0.0], np.arange(100.0, 251.0, 25.0), [cars['Co2'].max()])))).size()
    group_by_co = group_by_co.rename('Shares by Co2')

    plt.subplot(224)
    group_by_co.plot.pie(autopct='%1.0f%%')
    plt.axis('equal')
    ###

    plt.savefig('data/vehstats.png')
