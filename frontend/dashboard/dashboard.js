/* globals Chart:false, feather:false */

(function () {
  'use strict'

  feather.replace()

  // Graphs
  var ctx = document.getElementById('lineChart')
  


  // eslint-disable-next-line no-unused-vars
  var lineChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: [
        'Sunday',
        'Monday',
        'Tuesday',
        'Wednesday',
        'Thursday',
        'Friday',
        'Saturday'
      ],
      datasets: [{
        data: [
          15339,
          21345,
          18483,
          24003,
          23489,
          24092,
          12034
        ],
        lineTension: 0,
        backgroundColor: 'transparent',
        borderColor: '#007bff',
        borderWidth: 4,
        pointBackgroundColor: '#007bff'
      }]
    },
    options: {
      scales: {
        yAxes: [{
          ticks: {
            beginAtZero: false
          }
        }]
      },
      legend: {
        display: false
      }
    }
  })

  var ctx = document.getElementById('donutChart')
  var donutChart = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: [
        'Accepted',
        'Rejected',
        'Spoiled',
        'Other'
      ],
      datasets: [{
        data: [
          80,
          10,
          5,
          5
        ],
        backgroundColor: [
          'red', 'blue', 'yellow', 'green'
        ]
      }]
    },  options: Chart.defaults.doughnut
  })
})()
