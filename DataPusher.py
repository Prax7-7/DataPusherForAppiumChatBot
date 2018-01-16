import csv
import json

import requests
from pymongo import MongoClient


# method to read csv file ----------------------------------------------------------------------------------------------
def csv_reader(file_obj):
    """
    Read a csv file
    """
    reader = csv.reader(file_obj)
    stories = []
    rownum = 0

    for row in reader:
        if rownum == 0:
            header = row
        else:
            colnum = 0
            data = {}
            for col in row:
                data[header[colnum]] = col
                colnum += 1
            json_data = json.dumps(data)
            stories.append(json_data)
        rownum += 1
    insert_to_db(stories)
# ----------------------------------------------------------------------------------------------------------------------


# method returns the labeled version of a question  --------------------------------------------------------------------
def get_labeled_question(sentence):
    payload = {"sentences": sentence}
    response = requests.post("http://localhost:8001/core/posTagAndLabel", payload)
    return response.text
# ----------------------------------------------------------------------------------------------------------------------


# method inserts a list of labeled questions
def insert_labeled_question_to_db(story_id, sentence):
    payload = {"storyId": story_id, "labeledSentence": sentence}
    response = requests.post("http://localhost:8001/train/insertLabeledSentence", payload)
    if response.status_code == 200:
        print("successfully inserted labeled question")
    else:
        print("insert labeled question unsuccessful")
    
# ----------------------------------------------------------------------------------------------------------------------


# method inserts story to db -------------------------------------------------------------------------------------------
def insert_story_to_db(data):
    client = MongoClient('localhost:27017')
    db = client.MyBot
    story_id = db.story.insert_one(data).inserted_id
    return story_id
# ----------------------------------------------------------------------------------------------------------------------


# method get the list of questions, inlcudes actual question and training questions ------------------------------------
def get_questions_list(obj):
    obj = json.loads(obj, 'utf-8')
    questions = [obj['question_title'], obj['training_question1'], obj['training_question2'], obj['training_question3']]
    return questions
# ----------------------------------------------------------------------------------------------------------------------


# method returns  a list of labeled questions taking a list of questions as input --------------------------------------
def get_labeled_questions_list(questions_list):
    labaled_questions_list = []
    for question in questions_list:
        labaled_questions_list.append(get_labeled_question(question))
    return labaled_questions_list
# ----------------------------------------------------------------------------------------------------------------------


# method inserts a list labeled questions to db ------------------------------------------------------------------------
def insert_labeled_question_list_to_db(story_id, labeled_questions_list):
    for question in labeled_questions_list:
        insert_labeled_question_to_db(story_id, question)
# ----------------------------------------------------------------------------------------------------------------------


# method creates a model file for a given objectId ---------------------------------------------------------------------
def build_model(story_id):
    response = requests.post("http://localhost:8001/core/buildModel/"+str(story_id))
    if response.status_code == 200:
        print("successfully built model for story_id : " + story_id)
    else:
        print("build model unsuccessful for story_id : "+story_id)
# ----------------------------------------------------------------------------------------------------------------------


# method returns payload json of story to be pushed to db --------------------------------------------------------------
def get_story_object(obj):
    obj = json.loads(obj, 'utf-8')
    data = dict(storyName=obj['storyName'], intentName=obj['intentName'], apiTrigger=bool(0),
                speechResponse=obj['body'], parameters=[])
    print("Created story object : " + data['storyName'])
    return data
# ----------------------------------------------------------------------------------------------------------------------


# method performs pre-processing  and direct insertion of data to db ---------------------------------------------------
def insert_to_db(stories):

    for obj in stories:

        story_obj = get_story_object(obj)

        story_id = insert_story_to_db(story_obj)

        questions_list = get_questions_list(obj)

        labeled_questions_list = get_labeled_questions_list(questions_list)

        insert_labeled_question_list_to_db(story_id, labeled_questions_list)

        build_model(str(story_id))
# ----------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    csv_path = "/Users/prashanthsiddharamaiah/Development/DataPusher/InputCSV/InputData.csv"
    with open(csv_path, "rb") as f_obj:
        csv_reader(f_obj)
