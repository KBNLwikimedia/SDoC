# Add structured data to files on Wikimedia Commons from an Excel sheet

## What this script does
[This script](./WriteSDoCfromExcel_nopasswd.py) writes Property-Qid pairs from an Excel sheet to the [Structured Data](https://commons.wikimedia.org/wiki/Commons:Structured_data) of files on Wikimedia Commons.

For instance it can add [Q284865](https://www.wikidata.org/wiki/Q284865) to the [P180 property](https://www.wikidata.org/wiki/Property:P180) (*Depicts*) of the file https://commons.wikimedia.org/wiki/File:Atlas_Schoemaker-UTRECHT-DEEL1-3120-Utrecht,_Utrecht.jpeg from the Excel file [P180Inputfile.xlsx](P180Inputfile.xlsx)

Althought mainly intended to add P180-values in bulk, this script is also able to add Wikidata Qids to other properties (than P180) in the structured data.

## Configuration
The Python and the Excel files need to be in the same folder/directory.

As you can see from the example Excel [P180Inputfile.xlsx](P180Inputfile.xlsx?raw=true), the script expects 3 inputs, corresponding to column names in the sheet:
* **CommonsFile**:  the title of the file on Wikimedia Commons, eg. [File:Atlas Schoemaker-UTRECHT-DEEL1-3120-Utrecht, Utrecht.jpeg](https://commons.wikimedia.org/wiki/File:Atlas_Schoemaker-UTRECHT-DEEL1-3120-Utrecht,_Utrecht.jpeg).

  A handy way to get all files from a category, in this case [Category:Atlas_Schoemaker-Utrecht](https://commons.wikimedia.org/wiki/Category:Atlas_Schoemaker-Utrecht) is via the API call  https://commons.wikimedia.org/w/api.php?action=query&generator=categorymembers&gcmlimit=500&gcmtitle=Category:Atlas_Schoemaker-Utrecht&format=xml&gcmnamespace=6 and clean up that XML using a regexp. 

* **CommonsMid**: the media ID (M-number) of the file. It consists of 'M + Page ID', where the Page ID of the file is listed in the [Page information](https://commons.wikimedia.org/w/index.php?title=File:Atlas_Schoemaker-UTRECHT-DEEL1-3120-Utrecht,_Utrecht.jpeg&action=info), so in this case 41686589.<br/> 

  An easy way to find M-numbers (in bulk) from file titles is by using the [Minefield tool](https://hay.toolforge.org/minefield/). You can copy-paste the full *CommonsFile* column into the tool, run it and obtain a list of M-numbers ("mid" in the CSV) 
* **QidDepicts**: The Wikidata Q-numbers (Qids) of the things that are depicted in the files (in case P180 is used). Use one Qid per row. See the yellow, green, blue etc. rows for examples of multiple values for the same file. 

  In the Excel, the column *DepictsLabelForReconciliation* is a helper column, as input for reconciliation via OpenRefine, to find the Qids that correspond to the labels in that column. This column is not used in the script. 

Of course, you can modify the variable names in the script and the columns names in the Excel to your own needs/taste.

Additionally, in the script itself: 
* the **target property** to add Qids to must be set, the default is "P180"
* **Wikimedia credentials**: You can specify your Wikimedia username and passwd in the USER and PASS variables. If left blank, or if incorrect (eg. you entered an incorrect passwd), the edit will still be done, but will be shown as done from your IP address.

## Disclaimer & improvements
This script has not been not fully tested and/or is 100% reliable. You might encounter some errors when running it, especially for target properties other than P180 and messy Excel-inputs. Always test with a small number of Excel rows (*for i in range(0,3):*) to make sure everything works as expected before running the full sheet (*for i in range(0,len(df2)):*)

Feel free to improve it and/or [let me know](https://github.com/KBNLwikimedia/SDoC/issues) any bugs via Github.

## Credits
The script is written by [User:OlafJanssen](https://commons.wikimedia.org/wiki/User:OlafJanssen). It uses the *addClaim* function in [this script](https://github.com/multichill/toollabs/blob/1d5ef0ea24333a4918d388fe0fdade12d97b66ac/bot/erfgoed/wikidata_to_monuments_list.py) by [User:Multichill](https://commons.wikimedia.org/wiki/User:Multichill) 

## Reusing this script
Feel free to reuse, adapt, license = CC0




