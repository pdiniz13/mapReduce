var conn = new Mongo("127.0.0.1:3001");
db = conn.getDB("meteor");

var mapFunc = function() {
  emit(this.serviceTypeID, {count: 1, serviceTime: this.serviceTime, products: this.products});
};

var reduceFunc = function(serviceTypeID, info) {
  var reducedVal = {count: 0, serviceTime: 0, totalProductsCount: 0, totalPerItem: {}, ratingTotal:0};

  for (var idx = 0; idx < info.length; idx++) {
    reducedVal.count += info[idx].count;
    reducedVal.serviceTime += info[idx].serviceTime;
    reducedVal.totalProductsCount += info[idx].products.length - 1;
    reducedVal.ratingTotal += info[idx].ratingTotal;
    for (var x = 0, count = info[idx].products.length; x < count; x ++){
      var product = db.products.findOne({_id: reducedVal.totalPerItem[info[idx].products[x]]});
      if (reducedVal.totalPerItem[product]){
        reducedVal.totalPerItem[product] ++;
      }
      else {
        reducedVal.totalPerItem[product] = 0;
      }
    }
  }

  return reducedVal;
};

var finalizeFunc = function (state, reducedVal) {
  reducedVal.sedrviceTimeAvg = reducedVal.serviceTime/reducedVal.count;
  reducedVal.rating = reducedVal.ratingTotal/reducedVal.count;
  return reducedVal;
};


db.tickets.mapReduce(
  mapFunc,
  reduceFunc,
  {
    out: "allServicesStats",
    finalize: finalizeFunc
  }
);