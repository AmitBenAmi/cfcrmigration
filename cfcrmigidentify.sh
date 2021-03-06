#!/bin/bash

## codefresh API call
functionApiCall(){

    echo " api call for $1 \"API KEY*****\" $3"
    curl -X GET $1 \
                    -H 'Authorization: '$2 > $3 \
                 

  status=`cat $3 | jq '.status'`
  #echo $status
  if [ -n $status ] 
  then
    if [ "$status" != 'null' ] && [ $status -eq 401 ]
    
    then
        echo " Authenticaiton error.Please check your api key and try again"
        cat $3
            if  [ $4 ]
            then
                exit 1;
            fi
    
    else
      echo " rest api call successful"
    fi
 fi
} 

# Iterate for either piepline definition from YAML or from  meta data inline
function1 ()  {

      echo "starting retrieveing all repo urls"

      rm -f cfcrregistrfinder_step1.log
      rm -f cfcrregistrfinder_stpe1.err
      rm -r cfpips
      mkdir cfpips

      ##get all the pipeline ids
      pipsMetaUrl="https://g.codefresh.io/api/pipelines?limit=1000&offset=0"
      allPipsMetadata="cfpips/"$2"_allPipsMetadata"

      echo '****** Step .1) Invoking '$pipsMetaUrl' to collect all pipelines detailed metadata into a file '$allPipsMetadata"All.json"
      #rm cfpips/$allPipsMetadata".json"
      functionApiCall $pipsMetaUrl $1 $allPipsMetadata"All.json" true


      echo '****** Step 1) generating '$allPipsMetadata"_metadata.json file containing only pipeline metadata available"

      cat $allPipsMetadata"All.json" |jq '[.docs[]| {meta : {name: .metadata.name,project:.metadata.project,pipelineid:.metadata.id},pilinesteps:.spec.steps}]' > $allPipsMetadata"_metadata.json"
 
     # get the last build per pipeline

      echo '****** Step 2) generating '$allPipsMetadata"_metadata.json file containing only pipeline metadata available"

      #cat $allPipsMetadata".json" |jq '[.docs[]| {meta : {name: .metadata.name,project:.metadata.project,pipelineid:.metadata.id}}]' > $allPipsMetadata"_metadat.json"
      a=1
      jq -c '.[]' $allPipsMetadata"_metadata.json" | while read row; do
        pip_id=`echo  $row | jq -c '.meta.pipelineid'|sed 's/\"//g'`
        #   echo 'excrted pipid'$pip_id
        pip_name=`echo  $row | jq -c '.meta.name'|sed 's/\"//g'`
        pilinesteps=`echo  $row | jq -c '.pilinesteps'|sed 's/\"//g'`

       # echo $pilinesteps
        
        if [ $a -le $3 ]
        then
       
            a=`expr $a + 1`;
            pipBuildUrl="https://g.codefresh.io/api/workflow/?limit=1&page=1&pageSize=1&pipeline="$pip_id
            filaname="${pip_name////_}_$pip_id"
            mfilename="cfpips/"$filaname"_meta.json"
            functionApiCall $pipBuildUrl $1 $mfilename true
            finalWorkflowYaml=$(cat $mfilename| jq '.workflows.docs[0].finalWorkflowYaml' | sed 's/\\n/\'$'\n''/g' | sed -e 's/^"//' -e 's/"$//')
            #echo "finalWorkflowYaml is $finalWorkflowYaml"   
            if [ ! -z "$finalWorkflowYaml" ] && [ "$finalWorkflowYaml" != 'null' ]
            then
                yfilename="cfpips/"$filaname".yaml"

                echo "$finalWorkflowYaml" > $yfilename
                $(yq -j r $yfilename| jq '.| {spec:.steps}|.[]' > "cfpips/"$filaname".json")
            else
              echo "skipped to process the yaml from build. defaulting to metadata inline pipeline data" >> cfcrregistrfinder_step1.log
              
              #finalWorkflowYaml=$(echo  $row | jq -c '.meta.pipeyaml' | sed 's/\\n/\'$'\n''/g' | sed -e 's/^"//' -e 's/"$//')
              $(echo $pilinesteps > "cfpips/"$filaname".json")
            
            fi
        else
           echo "processing pipline no" $a  "pipelin id " $pip_id " name : " $pip_name " and breaking the loop as no of pipelines retrieved > limit passed in : " $3 
          break
        fi
      done
      echo "retrieved  " $a " no of pipelines yaml/json from the metadata into the folder cfpips"
       

}

## Search for build , push, pull steps.
function2(){


  a=1
  rm -f cfcrregistrfinder_step2.log
  rm -f cfcrregistrfinder_stpe2.err
  rm -f $2_pipidentification_Report.csv
  echo "PipelineID,PipelineName,Step type,StepName,Pipeline JSON File" >> $2_pipidentification_Report.csv
  allPipsMetadata=$2"_allPipsMetadata"
  a=1
  cat cfpips/$allPipsMetadata"_metadata.json"|jq -c '.[]'| while read row; do
    
    pip_id=`echo  $row | jq -c '.meta.pipelineid'|sed 's/\"//g'`

    if [ ! -z "$pip_id" ]
    then
        #echo 'excrted pipid'$pip_id
        pip_name=`echo  $row | jq -c '.meta.name'|sed 's/\"//g'`
        # if [ $a -le 1000 ]
        #     then
        #       echo "breaking the loop after processing row no : " $a  "reached the set limit : " $3
        #       break
        # else 
        #      echo "processing pipline no "$a" pipelin id: " $pip_id ", name : " $pip_name
            
        # fi
        echo "processing pipline no "$a" pipelin id: " $pip_id ", name : " $pip_name >> cfcrregistrfinder_step2.log
        a=`expr $a + 1`;


        filaname="${pip_name////_}_$pip_id"

        f="cfpips/"$filaname".json"
        fyaml="cfpips/"$filaname".json"
        if [[ -s $f ]]; then 
            #  echo "file has something"; 
                firstline=`tail -1 $f`
                echo "firstline" $firstline  >> cfcrregistrfinder_step2.log
                if [ "$firstline" != "null" ]; then
                
                    #echo 'next json file '$f ',cfcr references --> '`cat $f |jq '.|with_entries( select(.value.type == "build" or .value.registry == "cfcr" ))'`;
                    build_type_ref=`cat $f |jq '.|with_entries( select(.value.type == "build"))'`
                    push_cfcr_type_ref=`cat $f |jq '.|with_entries( select(.value.registry == "cfcr"))'`
                    

                    if [ "$build_type_ref" != "{}" ]; then
                        step_name=`echo $build_type_ref |jq  '.|keys[0]'`
                    
                        JSON_STRING=`echo  $build_type_ref |sed 's/\,/\^/g'`;
                    # echo $JSON_STRING 
                        nextpipoutput=$pip_id","$pip_name",build,"$step_name","$filaname".json"
                        echo $nextpipoutput >> $2_pipidentification_Report.csv
                    else
                        echo 'no type:build in pipeline '$pip_name >> cfcrregistrfinder_step2.log
                    fi

                    
                    if [ "$push_cfcr_type_ref" != "{}" ]; then
                        step_name=`echo $push_cfcr_type_ref |jq  '.|keys[0]'`
                        JSON_STRING=\'$push_cfcr_type_ref\'
                        #echo $JSON_STRING 
                        
                        nextpipoutput=$pip_id","$pip_name",push,"$step_name","$filaname".json"


                        echo $nextpipoutput >> $2_pipidentification_Report.csv
                    else
                    echo 'no push:type registry:cfcr in pipeline '$pip_name >> cfcrregistrfinder_step2.log
                    fi


                    cfcrrep=$(cat $f|grep -o '"r.cfcr.io/[^ ]*\"'|sed 's/\"//g'|head -1)

                    if [ ! -z "$cfcrrep" ]; then 
                        echo $pip_name' contains... reference to a r.cfcr.io image '$cfcrrep  >> cfcrregistrfinder_step2.log

                        
                        pull_cfcr_type_ref=`cat $f |jq --arg cfcrref $cfcrrep '.|with_entries( select(.value.image == $cfcrref))'`

                    # echo $pull_cfcr_type_ref
                    
                        if [ "$pull_cfcr_type_ref" != "{}" ]; then
                            step_name=`echo $pull_cfcr_type_ref |jq  '.|keys[0]'`
                            JSON_STRING=$(cat $pull_cfcr_type_ref| sed 's/\,/\^/g' |sed 's/\"/\"\"/g')
                            JSON_STRING=\'$pull_cfcr_type_ref\'
                            #echo $JSON_STRING 

                            nextpipoutput=$pip_id","$pip_name",pull,"$step_name","$filaname".json"
                            echo $nextpipoutput >> $2_pipidentification_Report.csv
                        else
                        #echo 'no push:type registry:cfcr in pipeline '$pip_name >> cfcrregistrfinder_step2.log
                            
                            pull_cfcr_pip=`cat $f |jq '.'`
                            nextpipoutput=$pip_id","$pip_name",pull,step_find_error,"$filaname".json"
                            echo $nextpipoutput >> $2_pipidentification_Report.csv
                        fi
                    else
                    echo $pip_name' does not contains reference to a r.cfcr.io image '$cfcrrep  >> cfcrregistrfinder_step2.log

                    fi


                else
                
                    echo "invalid json in $f"  >> cfcrregistrfinder_stpe2.err
                
                # head -5 fil1.yaml >>  cfcrregistrfinder_stpe2.err
                    echo "" >>  cfcrregistrfinder_stpe2.err

                fi
        else 
                echo "file : $f is empty"  >> cfcrregistrfinder_stpe2.err
            # head -5 fil1.yaml >>  cfcrregistrfinder_stpe2.err
                echo "" >>  cfcrregistrfinder_stpe2.err

        fi
    else
     echo 'skipped de to parse error' $row >> cfcrregistrfinder_stpe2.err
     
    fi


  done

}
# process a sample josn file for the build, push , pull patterns
function3(){

    f="SampleJson.json"
    if [[ -s $f ]]; then 
        #  echo "file has something"; 
            firstline=`tail -1 $f`
            echo "firstline" $firstline  
            if [ "$firstline" != "null" ]; then
               
                #echo 'next json file '$f ',cfcr references --> '`cat $f |jq '.|with_entries( select(.value.type == "build" or .value.registry == "cfcr" ))'`;
                # build_type_ref=`cat $f |jq '.|with_entries( select(.value.type == "build"))'`
                # push_cfcr_type_ref=`cat $f |jq '.|with_entries( select(.value.registry == "cfcr"))'`

                cfcrrep=$(cat $f|grep -o '"r.cfcr.io/[^ ]*\"'|sed 's/\"//g'|head -1)

                echo $cfcrrep

               pull_cfcr_type_ref=`cat $f |jq --arg cfcrref $cfcrrep '.|with_entries( select(.value.image == $cfcrref))'`

               echo $pull_cfcr_type_ref
                

                # if [ "$build_type_ref" != "{}" ]; then
                #     step_name=`echo $build_type_ref |jq  '.|keys[0]'`
                  
                #     JSON_STRING=`echo  $build_type_ref |sed 's/\,/\^/g'`;
                #    # echo $JSON_STRING 
                #     nextpipoutput=$pip_id","$pip_name",build,"$step_name","$JSON_STRING
                #     echo $nextpipoutput >> $2_pipidentification_Report.csv
                # else
                #     echo $build_type_ref
                # fi

                 
                


            else
             
                echo "invalid json in $f" 
               
              

            fi
      else 
            echo "file : $f is empty"; 
         

    fi
}

if [ $# -eq 3 ]; then
   # echo "Your command line contains $# arguments"
  
    # prepare data content. Metadata, yaml and json files for a pipeline
    function1 $1 $2 $3
    # run regex against the pipeline to identify the changes
    
    function2 $1 $2 $3
  #  function3 $1 $2 $3
else    
   echo "please run the script passing in codefresh api key as ./cfcrmigidentify.sh <api_key> account_shorthandname pipeline limit"
fi
