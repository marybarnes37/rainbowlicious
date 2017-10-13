# rainbowlicious

A rainbow predictor built on data science, Twilio and awesomeness. 

(also, a work in progress)

## Business Understanding

Who doesn’t take joy at the sight of a rainbow, or in pointing one out to others in their vicinity?

This project builds out two of the components that would be required to create a ‘Rainbow Alert’ phone app. The full version of the app has three components. First, it predicts the probability of witnessing a rainbow in the user’s geographic area on that day (a morning prediction and an afternoon prediction). Second, it offers a real-time probability of spotting a rainbow which is updated at intervals throughout the day (as long as the sun is below 42 degrees). Third, if a user spots a rainbow, takes a picture of it and uploads it to the app, the app will verify that the photo contains a rainbow and then send a “rainbow alert’ notification to other users in the geographic area. The purpose of validating the photos with an image classifier is to ensure that a user cannot set off an erroneous rainbow alert in their area. 

My capstone will tackle the two primary data science elements of the full Rainbow Alert app. First, I will build two predictive models for ‘probability of rainbow given weather conditions and time of day.’ One model will use forecast data and the other will use real-time observed data. I will start by modelling the Seattle area. Time allowing, I will expand the service by creating models for other cities.  Second, I will build the rainbow image classifier that will be used to verify the presence of a rainbow in user-uploaded photos. It is unlikely I will accomplish this second task during the two-week time frame.

I will design my model so that, once the app is functional, its predictive capability is improved by incorporating data about the weather conditions in which verified rainbows occur. 

Should I decide not to build the fully-functioning app, a predictive model for rainbows could still be a welcome addition to a standard weather forecast phone app. Cloudy with a chance of rainbows!



## Data Preparation

I am able to take a instagram post and retrieve date and location information from it. The Flickr API should make this piece of the pipeline even easier. After that, I am hoping the weather data is relatively clean and easy to access, but I still haven’t attempted it. 

I will probably follow the Raduga example and use radar images to locate areas where rainbows are possible and then further refine the prediction with the surface weather data matching the training set of times and locations garnered from the social media data..


## Modeling

I suspect that I will fit a logistic regression, a gradient boosted decision tree, and a random forest and use cross validation to choose the best. 

For the image classification model, I plan to use a neural net. 


## Evaluation

Both the predictive model and the image classifier can be evaluated via k-fold cross validation. 


## Deployment

My minimal viable product is a website that allows users to sign up with their mobile phone number to receive alerts when the probability of rainbows is above their user-defined threshold (twilio).

If I am also able to build the rainbow image classifier, I would like to come up with an app that notifies users when there is a confirmed rainbow sighting in their area. 
