from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.renderers import JSONRenderer
from rest_framework.parsers import JSONParser
# from PRIPS_workflow import run_workflow
from .src_demo_new.PRIIPS_workflow import run_workflow
import json

def calculate(request):
    if request.method == 'GET':
        print(json.loads(request.GET['json']))
        input = json.loads(request.GET['json'])
        result = run_workflow(input)
        return JsonResponse(result.to_json(orient='records'), safe=False)

    elif request.method == 'POST':
        data = JSONParser().parse(request)
        return JsonResponse(serializer.errors, status=400)

# Create your views here.
