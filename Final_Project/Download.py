# Download from SDSSDR18 all spectra from the match

import os
import time
import numpy as np
import pandas as pd
from astroquery.sdss import SDSS
from astropy.io import fits
from astropy.table import Table

specobj = pd.read_csv('/home/thara/Big/specobj_results.csv')

# Load the spectra information
spectra_info = specobj  # Replace with your actual DataFrame

# Define the output directory for spectra
output_dir = "/home/thara/Big/Spectra"
parquet_dir = "/home/thara/Big/Spectra_Parquet"  # Directory for Parquet files
os.makedirs(output_dir, exist_ok=True)  # Create the directory if it doesn't exist

# Define batch size and retry settings
batch_size = 1000  # Number of spectra to process in one batch
max_retries = 3  # Number of retries for failed downloads

# Initialize a list to log failed spectra
failed_spectra = []

# Process spectra in batches
for batch_start in range(0, len(spectra_info), batch_size):
    # Get the current batch
    batch = spectra_info.iloc[batch_start:batch_start + batch_size]
    print(f"Processing batch {batch_start // batch_size + 1} ({batch_start} to {batch_start + len(batch)} entries)...")
    
    for idx, row in batch.iterrows():
        plate, mjd, fiberid = row['plate'], row['mjd'], row['fiberid']
        filename = os.path.join(output_dir, f"spectrum_plate{plate}_mjd{mjd}_fiber{fiberid}.fits")
        
        # Define Parquet filename
        parquet_file = os.path.join(parquet_dir, f"spectrum_plate{plate}_mjd{mjd}_fiber{fiberid}.parquet")
        
        # Skip download if Parquet file already exists
        if os.path.exists(parquet_file):
            continue
        
        # Retry mechanism
        retries = 0
        success = False
        while retries < max_retries and not success:
            try:
                spectrum = SDSS.get_spectra(plate=plate, mjd=mjd, fiberID=fiberid)
                if spectrum:
                    # Save the spectrum to the output directory
                    spectrum[0].writeto(filename, overwrite=True)
                    success = True
                else:
                    print(f"  Spectrum not found: plate={plate}, mjd={mjd}, fiberid={fiberid}")
                    retries = max_retries  # Exit retry loop as it's a permanent failure
            except Exception as e:
                retries += 1
                print(f"  Error downloading spectrum: plate={plate}, mjd={mjd}, fiberid={fiberid}. Error: {e}. Retrying...")
                time.sleep(2)  # Short delay before retrying
        
        if not success:
            failed_spectra.append({'plate': plate, 'mjd': mjd, 'fiberid': fiberid})
            print(f"  Failed to download spectrum after {max_retries} retries: plate={plate}, mjd={mjd}, fiberid={fiberid}")
    
    print(f"Finished processing batch {batch_start // batch_size + 1}.")

# Save the failed spectra log
if failed_spectra:
    failed_df = pd.DataFrame(failed_spectra)
    failed_df.to_csv("failed_spectra.csv", index=False)
    print(f"Failed spectra logged to 'failed_spectra.csv'.")

print("All spectra processing completed.")