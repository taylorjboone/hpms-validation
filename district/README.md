# Generate District Template
    Generate the District Template to be sent to all Districts requesting the District data

## Requirements
   - Samples (current using a modifyed dump of the LRS HPMS sample layer )
   - LRS_Surface_type Layer
   - Pavement All roads file (current reading a copy of file the from "pavement_output_3_4_24/2023_COMBINED_ROUTES_DATA_ALL_3_4_24.xlsx")


## Instruction
    - Prepare the pavement file, export it to csv (through pandas or excel) and make sure to have the columns ROUTEID, BMP, and EMP. (necessary to remane to match it exacly)

    Run produce_district_template.py


### Result columns
   - Year_Record	
   - State_Code	
   - District	
   - Section_Length	
   - Sample_ID	
   - Comments	
   - MBI_SURFACE_TYPE	(obs: pavement file)
   - LRS_SURFACE_TYPE	
   - RouteID	
   - BMP	
   - EMP	
   - Year Last Improvement	
   - Year Last Constructed	
   - Last Overlay Thickness	
   - thickness Rigid	
   - Thickness Flex	
   - Base Type	
   - Base Thickness

