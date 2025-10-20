import { useEffect, useState } from "react";
import { getHealth } from "../utils/api";

function GetHealth() {
    const [data, setData] = useState(null);

    useEffect(() => {
        const fetchHealth = async () => {
            try {
                const response = await getHealth();
                setData(response);
            } catch (error) {
                console.error("Error fetching health data:", error);
            }
        };
        fetchHealth();
    }, []);

    return (
        <div>
            <h2>Health Status</h2>
            {data ? <pre>{JSON.stringify(data, null, 2)}</pre> : <p>Loading...</p>}
        </div>
    );
}

export default GetHealth;
