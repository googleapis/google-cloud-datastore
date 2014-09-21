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

var USAGE = {
  ADD_TODO: [
      '',
      'To add a todo, run:',
      '  $ datastore-todos ' + LIST + ' add "Get the mail"'
    ].join('\n'),

  HELP: [
      '',
      'Usage:',
      '  datastore-todos <todolist> <add|get|del|edit|ls|archive> [title|id] [new-title new-state]',
      '',
      'Examples:',
      '  datastore-todos MyTodos ls',
      '  datastore-todos MyTodos add "get the mail"',
      '  datastore-todos MyTodos edit {id} "get the mail" true',
    ].join('\n'),

  SHOW_TODOS: [
      '',
      'To see your list of todos, run:',
      '  $ datastore-todos ' + LIST + ' ls'
    ].join('\n')
};

var todos = new Todos(LIST);

todos
  .on('error', function(err) {
    console.log('Error:', err.message);
  })
  .on('todos fetched', function(entities) {
    if (entities.length === 0) {
      console.log('No todos found.');
      console.log(USAGE.ADD_TODO);
      return;
    }
    entities.forEach(function(entity, index) {
      var completed = entity.data.completed;
      var icon = [red('✖'), green('✔')][Number(completed)];
      console.log(icon + ' ' + bold(underline(entity.data.title)));
      console.log('    Mark as %s:', completed ? 'incomplete' : 'complete');
      console.log('    ' + cyan('$') + ' datastore-todos %s edit %s "%s" %s',
          LIST, entity.key.path.pop(), entity.data.title, String(!completed));
    });
  })
  .on('todos archived', function() {
    console.log('Todos successfully archived.');
    console.log(USAGE.SHOW_TODOS);
  })
  .on('todo added', function() {
    console.log('Todo successfully added.');
    console.log(USAGE.SHOW_TODOS);
  })
  .on('todo fetched', function(entity) {
    console.log('Todo successfully retrieved.');
    console.log(JSON.stringify(entity, null, 2));
  })
  .on('todo deleted', function() {
    console.log('Todo successfully deleted.');
    console.log(USAGE.SHOW_TODOS);
  })
  .on('todo edited', function() {
    console.log('Todo successfully edited.');
    console.log(USAGE.SHOW_TODOS);
  });

if (todos[ACTION]) {
  todos[ACTION].apply(todos, [].slice.call(process.argv, 4));
} else {
  console.log(USAGE.HELP);
}

function bold(str) {
  return '\u001b[1m' + str + '\u001b[22m';
}

function underline(str) {
  return '\u001b[4m' + str + '\u001b[24m';
}

function red(str) {
  return '\u001b[31m' + str + '\u001b[39m';
}

function green(str) {
  return '\u001b[32m' + str + '\u001b[39m';
}

function cyan(str) {
  return '\u001b[36m' + str + '\u001b[39m';
}