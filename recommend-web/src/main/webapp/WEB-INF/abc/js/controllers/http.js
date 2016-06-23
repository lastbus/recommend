/**
 * Created by MK33 on 2016/6/20.
 */
app.controller("httpCtrl", ['$scope', '$http', function($scope, $http){

    $scope.txt = 0
    $http.get('/angularJS').success(function(response){
        alert(JSON.stringify(response))
        $scope.txt = response
    }).error(function(response){
       $scope.txt = 'error: ' + response
    });


}])