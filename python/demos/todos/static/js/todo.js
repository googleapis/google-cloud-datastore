//
// Copyright 2013 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
angular.module('todo', ['ngResource'])
    .factory('Todo', function($resource) {
      var Todo = $resource('/todos');
      return Todo;
    })
    .controller('TodoCtrl', function($scope, Todo) {
      $scope.todos = Todo.query();

      $scope.addTodo = function() {
        var todo = new Todo();
        todo.text = $scope.todoText;
        todo.updating = true;
        todo.done = false;
        todo.$save();
        todo.state = 'saving';
        $scope.todos.push(todo);
        $scope.todoText = '';
      };

      $scope.change = function(todo) {
        todo.$save();
        todo.state = 'updating';
      };

      $scope.disabled = function(todo) {
        return todo.state !== undefined;
      };

      $scope.remaining = function() {
        var count = 0;
        angular.forEach($scope.todos, function(todo) {
          count += todo.done ? 0 : 1;
        });
        return count;
      };

      $scope.clear = function() {
        Todo.remove(function() {
          Todo.query(function(todos) {
            $scope.todos = todos;
          });
        });
      };
    });
