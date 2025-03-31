import mysql.connector
import yaml
import argparse
import time
import os

from datetime import timedelta
from obspy.clients.fdsn import Client
from obspy import Stream, UTCDateTime

def get_picks(start_time, end_time, conn):

    try:
        cursor = conn.cursor()
        start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
        end_time = end_time.strftime("%Y-%m-%d %H:%M:%S")
        arrival_query = """

        SELECT 
            Pick.waveformID_networkCode AS Network, 
            Pick.waveformID_stationCode AS Station, 
            Pick.waveformID_channelCode AS Channel, 
            Pick.time_value AS PickTime,
            Pick.time_value_ms as PickTime_ms
        FROM 
            Pick
        WHERE Pick.time_value > %s
        AND Pick.time_value < %s
        
        """
        cursor.execute(arrival_query, (start_time, end_time))
        picks = cursor.fetchall()
        return picks

    except mysql.connector.Error as e:
        print(f"Database error: {e}")
        return []

    finally:
        cursor.close()


def update_to_SW_View(output_dir, start_time, end_time, fdsn_server, conn):

    try:

        picks = get_picks(start_time, end_time, conn)

    except Exception:

        print("Database connection issue")
        return 1
    
    lockfile = open(os.path.join(output_dir, ".lock"), "w")
    lockfile.close()
    writefile = open(os.path.join(output_dir, "picks.txt"), "w")

    if picks:

        for pick in picks:

            network, station, channel, pick_time, pick_time_ms = pick
            pick_time = pick_time + timedelta(microseconds=pick_time_ms)
            formatted_dt = pick_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            print(f"{network}, {station}, {channel}, {formatted_dt}", file = writefile)

    else:

        print(f"No picks found from {start_time} to {end_time}", file = writefile)

    writefile.close()
    client = Client(fdsn_server)

    try:
        # Fetch station metadata
        inventory = client.get_stations(level="channel", starttime=start_time, endtime=end_time)
        print(f"Found {len(inventory)} networks with data.")

        for network in inventory:
            for station in network:
                try:
                    # Fetch waveform data for the station
                    st = Stream()
                    for channel in station.channels:
                        try:
                            tr = client.get_waveforms(
                                network.code, station.code, "*", channel.code, start_time, end_time
                            )
                            st += tr
                        except Exception as e:
                            print(f"Failed to fetch channel {channel.code}: {e}")

                    # Merge and save data by station
                    if st:
                        st.merge(method=1, fill_value=0)  # Merge overlapping traces
                        filename = f"{network.code}.{station.code}.msd"
                        filepath = os.path.join(output_dir, filename)
                        st.write(filepath, format="MSEED")
                        print(f"Saved {filepath}")

                except Exception as e:
                    print(f"Failed to process station {station.code}: {e}")

    except Exception as e:
        print(f"Failed to retrieve data: {e}")
        os.remove(os.path.join(output_dir, ".lock"))
        return 1

    os.remove(os.path.join(output_dir, ".lock"))
    return 0

def read_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def normal_mode(config):
    db_params = config.get("db_params", {})
    refresh_mins = float(config.get("refresh"))
    duration = float(config.get("duration"))
    output_dir = config.get("output_dir")
    fdsn_server = config.get("fdsn_server")
    os.makedirs(output_dir, exist_ok=True)
    try:
        conn = mysql.connector.connect(**db_params)
        #end_time = UTCDateTime.now()
        #start_time = end_time - timedelta(minutes=duration)
        while True:

            end_time = UTCDateTime.now()
            start_time = end_time - timedelta(minutes=duration)
            result = update_to_SW_View(output_dir, start_time, end_time, fdsn_server, conn)
            if result == 0:
                print(f"Data updated successfully. Next cycle is planned in {refresh_mins} minutes")
                time.sleep(refresh_mins * 60)
                start_time = end_time
            else:
                print(f"Data update failed. Next cycle is planned in {refresh_mins} minutes")
                if not conn.is_connected():
                    try:
                        conn.connect()
                    except Exception:
                        time.sleep(refresh_mins * 60)
                        continue

    except Exception:
        print("Database connection issue")

    finally:
        if 'connection' in locals() and conn.is_connected():
            conn.close()
            print("Database connection closed")

def offline_mode(config):

    db_params = config.get("db_params", {})
    fdsn_server = config.get("fdsn_server")
    output_dir = config["offline"]["output_dir"]
    from_time = UTCDateTime(config["offline"]["from_time"])
    to_time = UTCDateTime(config["offline"]["to_time"])
    os.makedirs(output_dir, exist_ok=True)

    try:
        conn = mysql.connector.connect(**db_params)
        result = update_to_SW_View(output_dir, from_time, to_time, fdsn_server, conn)
        if result == 0:
            print(f"Data updated successfully from {from_time} to {to_time} and stored in {output_dir}")
        else:
            print(f"Data update failed")

    except KeyboardInterrupt:
        print("Script interrupted by user")

    except mysql.connector.Error as e:
        print("Database connection error:", e)

    finally:
        if 'connection' in locals() and conn.is_connected():
            conn.close()
            print("Database connection closed")    

    return 0

def main():
    parser = argparse.ArgumentParser(description="FDSNWS to SW_View")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to YAML configuration file")
    parser.add_argument("--offline", action="store_true", help="Run in offline mode for a single download")
    args = parser.parse_args()
    config = read_config(args.config)
    if args.offline:
        offline_mode(config)
    else:
        normal_mode(config)

if __name__ == "__main__":
    main()