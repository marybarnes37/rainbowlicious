This repo includes the web scraping, data preparation, and model building code that I used to create a rainbow predictor for the Seattle area. Please see my repo at https://github.com/marybarnes37/rainbow_flask/ for information on the final implementation. 

## Business Understanding

Who doesn’t take joy at the sight of a rainbow, or in pointing one out to others in their vicinity?

This project builds out two of the components that would be required to create a ‘Rainbow Alert’ phone app. The full version of the app has three components. First, it predicts the probability of witnessing a rainbow in the user’s geographic area on that day (a morning prediction and an afternoon prediction). Second, it offers a real-time probability of spotting a rainbow which is updated at intervals throughout the day (as long as the sun is below 42 degrees). Third, if a user spots a rainbow, takes a picture of it and uploads it to the app, the app will verify that the photo contains a rainbow and then send a “rainbow alert’ notification to other users in the geographic area. The purpose of validating the photos with an image classifier is to ensure that a user cannot set off an erroneous rainbow alert in their area. 

The code in this repo tackles the first data science elements of the full Rainbow Alert app: it returns the‘probability of rainbow given weather conditions and time of day’ in Seattle.

Should I decide not to eventually build the fully-functioning app, a predictive model for rainbows could still be a welcome addition to a standard weather forecast phone app. Cloudy with a chance of rainbows!


## Web Scraping 

The [instagram_sraper folder](https://github.com/marybarnes37/rainbowlicious/tree/master/instagram_scraper) includes code for gathering location and timestamp information from Instagram posts with a given hashtag. I did not end up using this data because I was not confident in making sense of Instagrams timestamp format. 

The [flickrsraper folder](https://github.com/marybarnes37/rainbowlicious/tree/master/flickrscraper) includes code for gathering location and timestamp information from Flickr posts with a given tag from the Flickr API. I used weather data from these timestamps to train my model.

The [metar folder](https://github.com/adam-p/markdown-here/wiki/Markdown-Cheatsheet#links) contains data files that list METAR weather stations. This data could be used to extend the rainbow predictor to areas outside of Seattle. It was not necessary for the Seattle-only implementation. 

## Data Preparation 

I was given access to a non-public Weather API that allowed me to pull in weather data from specified times. 

The [flickrsraper folder](https://github.com/marybarnes37/rainbowlicious/tree/master/flickrscraper) contains all of the data preparation files for the Seattle implementation. 

## Modeling and Evaluation

In a Jupyter Notebook, I fit a logistic regression, a gradient boosted decision tree, and a random forest and use cross validation to choose the best. 

The [pickles folder](https://github.com/marybarnes37/rainbowlicious/tree/master/pickles) contains pickled dictionaries and trained models that are backups. They were originally created in a Jupyter notebook. 


## Deployment

The Seattle alert system is live at infinityrainbows.com. 

In the future, I would like to set up Twilio alerts so that users can receive SMS notifications. 

If I am also able to build the rainbow image classifier, I would like to come up with an app that notifies users when there is a confirmed rainbow sighting in their area. 
