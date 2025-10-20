import { useState } from "react";
import GetHealth from "../components/GetHealth"; 

function Home() {
  const [data, setData] = useState(null);

  return (
    <div>
      <h1>Home</h1>
      <GetHealth />
    </div>
  );
}

export default Home;
