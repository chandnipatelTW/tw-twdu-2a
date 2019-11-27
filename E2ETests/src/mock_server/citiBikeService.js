const mbHelper = require('./mountebankHelper');
const settings = require('./settings');
const sfData = require('../../data/citibikeSFData.json')

function addService() {
    const stubs = [
        {
            predicates: [ {
                equals: {
                    method: "GET",
                    "path": "/networks/ford-gobike"
                }
            }],
            responses: [
                {
                    is: {
                        statusCode: 200,
                        headers: {
                            "Content-Type": "application/json"
                        },
                        body: JSON.stringify(sfData)
                    }
                }
            ]
        }
    ];

    const imposter = {
        port: settings.citibankServicePort,
        protocol: 'http',
        stubs: stubs
    };

    return mbHelper.postImposter(imposter);
}

module.exports = { addService };