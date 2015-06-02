var conn = new Mongo("127.0.0.1:3001");
var db = conn.getDB("meteor");

var mapFunc = function() {
  emit(this.agentID, {count: 1, serviceTime: this.serviceTime, products: this.products, agent: this.agentID});
};

var reduceFunc = function(agentID, info) {
  var reducedVal = {count: 1, serviceTime: 0, totalProductsCount: 0, totalPerItem: {}, ratingTotal:0};
  for (var idx = 0, count = info.length; idx < count; idx++){
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
  reducedVal.serviceTimeAvg = reducedVal.serviceTime/reducedVal.count;
  reducedVal.rating = reducedVal.ratingTotal/reducedVal.count;
  return reducedVal;
};

db.tickets.mapReduce(
  mapFunc,
  reduceFunc,
  {
    out: "allUsersStats",
    finalize: finalizeFunc
  }
);