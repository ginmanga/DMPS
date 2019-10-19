



CRSPLINK.loc[CRSPLINK.LINKENDDT == 'E', 'LINKENDDT'] = 20191012
CRSPLINK = CRSPLINK.dropna(subset=['LINKENDDT'])
CRSPLINK = CRSPLINK.astype({'LINKDT':'int64', 'LINKENDDT':'int64'})

COMPUCRSPLI = CRSPLINK[['gvkey','LPERMNO','LPERMCO','LINKDT','LINKENDDT']]

CRSPLINKs = CRSPLINK[['gvkey','LPERMNO','LPERMCO','LINKDT', 'LINKENDDT']]

LINKDT = pd.pivot_table(CRSPLINKs, values=['LINKDT'], columns=['gvkey','LPERMNO','LPERMCO'],
                           aggfunc={'LINKDT': min}).reset_index()

LINKENDDT = pd.pivot_table(CRSPLINKs, values=['LINKENDDT'], columns=['gvkey','LPERMNO','LPERMCO'],
                           aggfunc={'LINKENDDT': max}).reset_index()

CRSPLINKY= pd.merge(CRSPLINK,
                      LINKDT[['gvkey','LPERMNO','LPERMCO', 0]],
                      left_on=['gvkey','LPERMNO','LPERMCO'],
                      right_on = ['gvkey','LPERMNO','LPERMCO'], how='left')
CRSPLINKY= pd.merge(CRSPLINKY,
                      LINKENDDT[['gvkey','LPERMNO','LPERMCO', 0]],
                      left_on=['gvkey','LPERMNO','LPERMCO'],
                      right_on = ['gvkey','LPERMNO','LPERMCO'], how='left')

CRSPLINKY = CRSPLINKY.drop_duplicates(subset=['gvkey','LPERMNO','LPERMCO'])


CRSPLINKY = CRSPLINKY.drop(['LINKDT','LINKENDDT'], axis = 1)
CRSPLINKY = CRSPLINKY.rename(columns={"0_x": "LINKDT", "0_y": "LINKENDDT"})

CRSPLINKY.to_csv(os.path.join(datadirectory, "collapsedlink.csv"))


