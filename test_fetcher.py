from src.station_features import build_station_features
station_df = build_station_features()

sample = [
    'Grand Central-42 St (S,4,5,6,7)',
    'Times Sq-42 St (N,Q,R,W,S,1,2,3,7)/42 St (A,C,E)',
    '34 St-Herald Sq (B,D,F,M,N,Q,R,W)',
    '14 St-Union Sq (L,N,Q,R,W,4,5,6)',
    'Fulton St (A,C,J,Z,2,3,4,5)',
    'Atlantic Av-Barclays Ctr (B,D,N,Q,R,2,3,4,5)',
    'Flushing-Main St (7)',
    '74-Broadway (7)/Jackson Hts-Roosevelt Av (E,F,M,R)',
    'Bay Ridge-95 St (R)',
    'Canarsie-Rockaway Pkwy (L)',
    '161 St-Yankee Stadium (B,D,4)',
    'Fordham Rd (B,D)',
    'Jamaica Center-Parsons/Archer (E,J,Z)',
    'Forest Hills-71 Av (E,F,M,R)',
    'Astoria-Ditmars Blvd (N,W)',
    '86 St (4,5,6)',
    'Court St (R)/Borough Hall (2,3,4,5)',
    'Church Av (B,Q)',
    'Pelham Bay Park (6)',
    'Inwood-207 St (A)'
]

# Check which station names exist in actual data
from src.query import get_connection
conn = get_connection()
actual_stations = conn.execute("SELECT DISTINCT station_complex FROM ridership").df()['station_complex'].tolist()

missing = [s for s in sample if s not in actual_stations]
print(f"Missing stations: {missing}")