#! /usr/bin/env node

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

var Todos = require('./');

var LIST = process.argv[2];
var ACTION = process.argv[3];

var USAGE = [
  'Usage:',
  '  datastore-todos <todolist> <add|get|del|edit|ls|archive> [title|id] [new-title new-state]',
  '',
  'Examples:',
  '  datastore-todos MyTodos ls',
  '  datastore-todos MyTodos add "get the mail"',
  '  datastore-todos MyTodos edit {id} "get the mail" true',
].join('\n');

var todos = new Todos(LIST);

todos
  .on('error', function(err) {
    console.log('Error:', err.message);
  })
  .on('todos fetched', function(entities) {
    console.log(JSON.stringify(entities, null, 2));
  })
  .on('todos archived', function() {
    console.log('Todos successfully archived.');
  })
  .on('todo added', function(entities) {
    console.log('Todo successfully added.');
    console.log(JSON.stringify(entities, null, 2));
  })
  .on('todo fetched', function(entity) {
    console.log('Todo successfully retrieved.');
    console.log(JSON.stringify(entity, null, 2));
  })
  .on('todo deleted', function() {
    console.log('Todo successfully deleted.');
  })
  .on('todo edited', function(entity) {
    console.log('Todo successfully edited.');
    console.log(JSON.stringify(entity, null, 2));
  });

if (todos[ACTION]) {
  todos[ACTION].apply(todos, [].slice.call(process.argv, 4));
} else {
  console.log(USAGE);
}