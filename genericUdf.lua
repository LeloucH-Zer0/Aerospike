function string:split( inSplitPattern, outResults )
  if not outResults then
    outResults = { }
  end
  local theStart = 1
  local theSplitStart, theSplitEnd = string.find( self, inSplitPattern, theStart )
  while theSplitStart do
    table.insert( outResults, string.sub( self, theStart, theSplitStart-1 ) )
    theStart = theSplitEnd + 1
    theSplitStart, theSplitEnd = string.find( self, inSplitPattern, theStart )
  end
  table.insert( outResults, string.sub( self, theStart ) )
  return outResults
end





function get_results(stream,filters,groupbys,projections)


  local function map_profile(record)
    local projectionsTable = string.split(projections,"|")
    local a = map {} 
    for i=1,#projectionsTable do
      a[projectionsTable[i]]=record[projectionsTable[i]]
    end
    return a
  end


  local function apply_groupby(resultMap, curRecord)
    local groupbysTable = string.split(groupbys,"|")
    local groupbysColumnName = groupbysTable[1];
    local groupbysColumnVal = groupbysTable[2];

-- validate that if given groupby is timestamp then its value is integer and not string
--    if(groupbysColumnVal ~= null and type(groupbysColumnVal) ~= 'number') then 
--      groupbysColumnVal = null
--    end

    local cn = null
    if(groupbysColumnVal == null) then
      cn = curRecord[groupbysColumnName]
    else
      cn = curRecord[groupbysColumnName]
      cn = (cn - (cn % groupbysColumnVal))
    end

    local map_record = resultMap[cn] 

    local projectionsTable = string.split(projections,"|")
    if map_record == null then
      map_record = map {}
      for i=1,#projectionsTable do
        map_record[projectionsTable[i]]=curRecord[projectionsTable[i]]
      end
    else
      for i=1,#projectionsTable do
        if(curRecord[projectionsTable[i]]~=nil) then
          map_record[projectionsTable[i]]=(map_record[projectionsTable[i]] or 0)+curRecord[projectionsTable[i]]
        end
      end
    end

    resultMap[cn] = map_record
    return resultMap
  end

  local function MergeRecords(a, b)
    local projectionsTable = string.split(projections,"|")
    for i=1,#projectionsTable do
      if(b[projectionsTable[i]]~=nil) then
        a[projectionsTable[i]] = (a[projectionsTable[i]] or 0) + b[projectionsTable[i]]
      end
    end
    return a
  end

  local function final_merge(a, b)
    return map.merge(a, b, MergeRecords)
    --return a
  end

  local function apply_filters(record)
    print 'apply_filters'
    local myTable = string.split( filters, "|")
    local is_valid=true;
    for i = 1, (#myTable)/3 do
      if(myTable[i*3-1] == 'eq') then
        -- if(record[myTable[i*3-2]] == myTable[i*3]) then
        --   is_valid=true
        -- else
        --   is_valid=false
        --   break
        -- end
        local equalsTable = string.split(myTable[i*3],"#")
        local equalsCheck = false
        for equalsLoopVar=1,#equalsTable do
          if(record[myTable[i*3-2]]== equalsTable[equalsLoopVar]) then
            equalsCheck=true
            break
          end
        end
        if(equalsCheck == true) then
          is_valid=true
        else
          is_valid=false
          break
        end    
      elseif(myTable[i*3-1] == 'g') then
        if(record[myTable[i*3-2]] > myTable[i*3]) then
          is_valid=true
        else
          is_valid=false
          break
        end
      elseif(myTable[i*3-1] == 'geq') then
        if(record[myTable[i*3-2]] >= myTable[i*3]) then
          is_valid=true
        else
          is_valid=false
          break
        end
      elseif(myTable[i*3-1] == 'l') then
        if(record[myTable[i*3-2]] < myTable[i*3]) then
          is_valid=true
        else
          is_valid=false
          break
        end
      elseif(myTable[i*3-1] == 'leq') then
        if(record[myTable[i*3-2]] <= myTable[i*3]) then
          is_valid=true
        else
          is_valid=false
          break
        end   
      end
    end
    return is_valid
  end
  if(groupbys~='empty') then
    return stream : filter(apply_filters) : aggregate(map(), apply_groupby) : reduce(final_merge)
  else
    return stream : filter(apply_filters) : map(map_profile)
  end
--  return stream : filter(apply_filters) : aggregate(map(), apply_groupby) : reduce(final_merge)
end