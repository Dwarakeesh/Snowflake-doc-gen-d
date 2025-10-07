from flask import Flask,request,jsonify
importrequests,os
app=Flask(__name__)
@app.route('/billing/dryrun',methods=['POST'])
defdryrun(): payload=request.json;res=requests.post(os.getenv('SNOWFLAKE_API_URL')+'/callProcedure',json={'name':'AI_FEATURE_HUB.RUN_BILLING_WRAPPER','args':[payload]},timeout=30);return jsonify(res.json())
if__name__=='__main__':app.run(host='0.0.0.0',port=8082)