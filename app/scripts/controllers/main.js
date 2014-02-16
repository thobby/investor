angular.module('investorApp')
  .controller('MainCtrl', ['$log', '$http', '$scope',
      function ($log, $http, $scope) {
      'use strict';

      $http(
        {
          method: "GET",
          url: "/linearRegression"
        }
      ).success(function (data) {
        // $log.debug('Linear regression response: ' + data);
        $scope.linearRegression = data;
      }).error(function (data) {
        $log.error("Error ");
      });
      $scope.gridOptions = { data: 'linearRegression' };
    }]);