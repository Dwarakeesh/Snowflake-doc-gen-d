from flask import Flask,request,jsonify
importrequests,os
app=Flask(__name__)
@app.route('/evidence/export',methods=['POST'])
defexport(): bundle_id=request.json.get('bundle_id'); stage=request.json.get('stage','@AI_FEATURE_HUB.ARCHIVE_STAGE'); # call Snowflake procedure via middleware layer return jsonify({'started':True,'bundle_id':bundle_id})
if__name__=='__main__': app.run(host='0.0.0.0',port=8090)