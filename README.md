# PatentsView Assignee Hand Disambiguation
Codebase for algorithmically setting up the manual assignee labeling tasks.

## Summary

|  |  |
|---|---|
| DSAA Team Lead | Sarvo Madhavan | 
| Project Tier | Tier 2 | 
| DSAA Team Members | - Olivier Binette (former AIR employee)<br>- Sarvo Madhavan (Senior Data Scientist) <br>- Siddharth Engineer (Data Scientist Assistant) |
| Client(s) | PatentsView (USPTO) |
| Project Start Date | 07/26/2023 |
| Project End Date | None |
| Status | In progress (awaiting hand-disambiguation process) |                                                                 

 ## Raw Materials In
Connection to AIR's AWS database of PatentsView data for sampling mention IDs and pulling assignee information.

## Result Out
Following the steps below, we will sample 10k mention IDs from the assignee table.
After sampling mention IDs, we will generate an output dataframe which includes:
- disambiguated assignee information
- patent information
- all rows are listed in the comments for `assignee_data()` in the `assignee.py` file

## Usage/Examples

First we'll need to set up a the database connection for pulling data from AWS.
In this directory, create a `.env` file with the following fields:
```
user = USERNAME
password = PASSWORD
hostname = HOSTNAME
dbname = algorithms_assignee_labeling
```

This is required for the `assignee.py` data pulls.
However, we can still get our sample mention IDs by running sample.py without this DB connection.
In your terminal, run:
```
python3 sample.py
```

Feel free to adjust the main method to run different commands in the `assignee.py` file.
To just get a populated dataset which is ready for manual assignee labeling, the main method is already configured so simply run the following in terminal:
```
python3 assignee.py
```

## FAQ

#### Q: Does it work well on both Windows and Mac?
A: You're hilarious

## Support
For support, email fake@fake.com for a response that is sure to be real

## License
Apparently this is a thing. 

### Roadmap

- Additional browser support
- Add more integrations
- Cat tree for Sami

## Appendix

Any additional information goes here. Could be related projects, helpful links to packages used, etc.
