const app = document.getElementById('stats')

const container = document.createElement('div')
container.setAttribute('class', 'container')

app.appendChild(container)

// Create a request variable and assign a new XMLHttpRequest object to it.
var request = new XMLHttpRequest()

// Open a new connection, using the GET request on the URL endpoint
request.open('GET', 'https://60663eecb8fbbd0017568315.mockapi.io/api/v1/BallotStats', true)

request.onload = function () {
    // Begin accessing JSON data here

    var data = JSON.parse(this.response)
    if (request.status >= 200 && request.status < 400) {
        data.forEach((stat) => {
            const card = document.createElement('div')
            card.setAttribute('class', 'card')

            const h1 = document.createElement('h1')
            h1.textContent = 'Total Accepted'

            const p = document.createElement('p')
            p.textContent = stat.total_accepted

            container.appendChild(card)
            card.appendChild(h1)
            card.appendChild(p)
        })
      } else {
        console.log('error')
      }
}

// Send request
request.send()



  
