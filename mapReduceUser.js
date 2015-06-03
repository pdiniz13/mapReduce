var mapReduce = function(indexName, outputFile, range) {
  var conn = new Mongo("127.0.0.1:3001");
  var db = conn.getDB("meteor");

  var mapFunc = function (indexName) {
    emit(this[indexName], {count: 1, serviceTime: this.serviceTime, products: this.products});
  };

  var reduceFunc = function (agentID, info) {
    var reducedVal = {count: 1, serviceTime: 0, totalProductsCount: 0, totalPerItem: {}, ratingTotal: 0};
    for (var idx = 0, count = info.length; idx < count; idx++) {
      reducedVal.count += info[idx].count;
      reducedVal.serviceTime += info[idx].serviceTime;
      reducedVal.ratingTotal += info[idx].ratingTotal;
      if (info[idx].products) {
        reducedVal.totalProductsCount += info[idx].products.length - 1;
        for (var x = 0, count1 = info[idx].products.length; x < count1; x++) {
          if (reducedVal.totalPerItem[reducedVal.totalPerItem[info[idx].products[x]]]) {
            reducedVal.totalPerItem[reducedVal.totalPerItem[info[idx].products[x]]]++;
          }
          else {
            reducedVal.totalPerItem[reducedVal.totalPerItem[info[idx].products[x]]] = 0;
          }
        }
      }
    }
    return reducedVal;
  };

  var finalizeFunc = function (key, reducedVal) {
    reducedVal.serviceTimeAvg = reducedVal.serviceTime / reducedVal.count;
    reducedVal.rating = reducedVal.ratingTotal / reducedVal.count;
    return reducedVal;
  };

  var queryOptions = {
    out:outputFile,
    finalize: finalizeFunc
  };
  if (range){
    queryOptions.query = {
      updatedAt: {
        $gt: range[0],
        $lt: range[1]
      }
    }
  }
  var newFunc = function(indexName){
    var indexName = indexName;
    function mapFunc(){
      var newName = indexName;
      emit(this[newName], {count: 1, serviceTime: this.serviceTime, products: this.products});
    }
    return mapFunc;
  };
  db.tickets.mapReduce(
    mapFunc,
    reduceFunc,
    queryOptions
  );

  db.tickets.mapReduce(

  );
};

mapReduce("agentID", "allAgentStats");
var dateRanges = {
  januaryAgentStats2015: [new Date(2015,0,1), new Date (2015,0,31)],
  februaryAgentStats2015: [new Date(2015,1,1), new Date (2015,1,28)],
  marchAgentStats2015: [new Date(2015,2,1), new Date (2015,2,31)],
  aprilAgentStats2015: [new Date(2015,3,1), new Date (2015,3,30)],
  mayAgentStats2015: [new Date(2015,4,1), new Date (2015,4,31)],
  juneAgentStats2015: [new Date(2015,5,1), new Date (2015,5,30)],
  julyAgentStats2015: [new Date(2015,6,1), new Date (2015,6,31)],
  augustAgentStats2015: [new Date(2015,7,1), new Date (2015,7,31)],
  septemberAgentStats2015: [new Date(2015,8,1), new Date (2015,8,30)],
  octoberAgentStats2015: [new Date(2015,9,1), new Date (2015,9,31)],
  novemberAgentStats2015: [new Date(2015,10,1), new Date (2015,10,30)],
  decemberAgentStats2015: [new Date(2015,11,1), new Date (2015,11,31)]
};
for (key in dateRanges){
  mapReduce("agentID", key, dateRanges[key]);
}