import md from '../readme.md';
import ReactMarkdown from 'react-markdown'
import { Header } from './Header';
import { useState } from 'react';

export function Readme(props: {}){
    let [state, setState] = useState<any>(null);
    if (state == null){
        fetch(md)
        .then(async (response) => {
                let mdtext = await response.text();
                setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
            }
        )
    }
    return (
        <>
            <Header />
            {state}
        </>
    )
}