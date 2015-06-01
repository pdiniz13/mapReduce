/**
 * Created by ppp on 5/31/2015.
 */


var mapFunc = function() {
  emit(this.serviceTypeID, {count: 1, serviceTime: this.serviceTime});
};

var reduceFunc = function(serviceTypeID, info) {
  reducedVal = {count: 0, serviceTime: 0};

  for (var idx = 0; idx < info.length; idx++) {
    reducedVal.count += info[idx].count;
    reducedVal.serviceTime += info[idx].serviceTime;
  }

  return reducedVal;
};

var finalFunc = function (state, reducedVal) {
  reducedVal.avg = reducedVal.serviceTime/reducedVal.count;
  return reducedVal;
};